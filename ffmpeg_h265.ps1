<#
.SYNOPSIS
  Parallel ffmpeg + NVENC conversion with bounded concurrency and per-job progress.
.DESCRIPTION
  Discovers files, precomputes encode work items, and runs up to -MaxParallelJobs ffmpeg
  processes concurrently. Uses a synchronized hashtable for progress state and renders
  a parent + per-job Write-Progress display. Logging is job-scoped and thread-safe.
.PARAMETER Path
  Required for encode and -Analyze runs; optional for -Compact and standalone -ViewReport
.PARAMETER MaxParallelJobs
  Maximum number of concurrent encoding jobs (1-4). Values above 4 are clamped to 4.
.PARAMETER ShowOutputCmd
  Display FFmpeg commands being executed
.PARAMETER NoProgress
  Disable progress bars
.PARAMETER ResizeResolution
  Target resolution (e.g., "1920:1080")
.PARAMETER ForceResize
  Force resize even if already at target resolution
.PARAMETER RetainAspect
  Maintain aspect ratio when resizing
.PARAMETER CanScaleUp
  Allow upscaling videos
.PARAMETER CanScaleDown
  Allow downscaling videos
.PARAMETER CQPRateControl
  Constant Quality (CQP) value (lower = higher quality, 18-32 typical)
.PARAMETER BitrateControl
  Target bitrate (e.g., "6M" for 6 Mbps) instead of CQP
.PARAMETER ForceConvert
  Re-encode files already in H.265 format
.PARAMETER CanReprocess
  Allow reprocessing of previously completed files
.PARAMETER SkipMoveOnCompletion
  Don't move files to processed folder after conversion
.PARAMETER SkipArrRefresh
  Skip Radarr/Sonarr refresh after completion
.PARAMETER LogEnabled
  Enable file logging
.PARAMETER LogVerbose
  Enable verbose logging
.PARAMETER ExitOnError
  Stop all processing if any job fails
.PARAMETER SortExpression
  Sort order for processing files
.PARAMETER UserFilter
  Custom filter for selecting files
.PARAMETER AudioLang
  ISO 639-2 audio language codes to keep; supports one or many values
.PARAMETER SubLang
  ISO 639-2 subtitle language codes to keep; supports one or many values
.PARAMETER LastRunDate
  Only process files modified after this date
.PARAMETER SkipFileLock
  Skip file lock check (process files even if in use)
.PARAMETER ConfigPath
  Directory containing ffmpeg_h265.config.json (overrides auto-discovery for encode/analyze/compact modes)
.PARAMETER LogPath
  Log output path override for the current run (accepts directory or full log file path; takes precedence over FFENC_LOG_PATH and config log_path)
.PARAMETER Encoder
  Encoder profile: auto, nvenc, amf, qsv, software
.PARAMETER Analyze
  Analyze metadata and generate report files without encoding
.PARAMETER ViewReport
  Launch report viewer helper (serve_report.ps1 -Report runtime) and open the runtime report; when used alone it does not require -Path or config loading
.PARAMETER HashAlgorithm
  Analyze hash strategy: size-mtime (fastest, default) or crc32 using the script's built-in CRC32 implementation
.PARAMETER Compact
  Compact existing metadata report data (remove deleted files)
.EXAMPLE
  .\ffmpeg_h265.ps1 -Path "V:\Media\TV Shows"
  Convert all video files in a directory using default settings.
.EXAMPLE
  .\ffmpeg_h265.ps1 -Path "V:\Media\Movies" -MaxParallelJobs 4 -CQPRateControl 26
  Converts movies with 4 parallel jobs and CQP quality level 26 (lower = higher quality).
.EXAMPLE
  .\ffmpeg_h265.ps1 -Path "V:\Videos" -ResizeResolution "1920:1080" -RetainAspect -CanScaleDown
  Resizes videos to 1080p, maintaining aspect ratio, only if the source is larger than 1080p.
.EXAMPLE
  .\ffmpeg_h265.ps1 -Path "V:\Media\TV Shows" -BitrateControl "6M" -ForceConvert -MaxParallelJobs 2
  Forces re-encoding using 6 Mbps bitrate limit, even if files are already H.265 encoded.
.EXAMPLE
  .\ffmpeg_h265.ps1 -Path "Z:\Movies" -Analyze
  Analyze metadata and generate report files without encoding.
.EXAMPLE
  .\ffmpeg_h265.ps1 -ViewReport
  Launch report viewer and open the runtime report without requiring -Path or config.
#>

param(
  # Runtime-scope parameters belong here (scan behavior, filters, encoding, logging toggles).
  # Environment/deployment settings (paths, endpoints, API keys) are intentionally
  # resolved from env/config to avoid a duplicated parameter surface.
  [Parameter(Mandatory = $false)][string] $Path,
  [Parameter(Mandatory = $false)][int] $MaxParallelJobs = 3,
  [Parameter(Mandatory = $false)][switch] $ShowOutputCmd,
  [Parameter(Mandatory = $false)][switch] $NoProgress,
  [Parameter(Mandatory = $false, HelpMessage = "1920:1080")][string] $ResizeResolution = "",
  [Parameter(Mandatory = $false)][switch] $ForceResize,
  [Parameter(Mandatory = $false)][switch] $RetainAspect,
  [Parameter(Mandatory = $false)][switch] $CanScaleUp,
  [Parameter(Mandatory = $false)][switch] $CanScaleDown,
  [Parameter(Mandatory = $false, HelpMessage = "ex. 28")][string] $CQPRateControl = "28",
  [Parameter(Mandatory = $false, HelpMessage = "ex. 6M")][string] $BitrateControl = "",
  [Parameter(Mandatory = $false)][switch] $ForceConvert,
  [Parameter(Mandatory = $false)][switch] $CanReprocess,
  [Parameter(Mandatory = $false)][switch] $SkipMoveOnCompletion,
  [Parameter(Mandatory = $false)][switch] $SkipArrRefresh,
  [Parameter(Mandatory = $false)][switch] $LogEnabled,
  [Parameter(Mandatory = $false)][switch] $LogVerbose,
  [Parameter(Mandatory = $false)][switch] $ExitOnError,
  [Parameter(Mandatory = $false)][hashtable] $SortExpression = @{ e = 'Name'; Descending = $false },
  [Parameter(Mandatory = $false)][scriptblock] $UserFilter = { $_.Length -ne -1 },
  [Parameter(Mandatory = $false)][string[]] $AudioLang = @("eng", "jpn"),
  [Parameter(Mandatory = $false)][string[]] $SubLang = @("eng"),
  [Parameter(Mandatory = $false)][datetime] $LastRunDate,
  [Parameter(Mandatory = $false)][switch] $SkipFileLock = $false,
  [Parameter(Mandatory = $false, HelpMessage = "Directory containing the .config.json file")][string] $ConfigPath = "",
  [Parameter(Mandatory = $false, HelpMessage = "Directory where log files will be stored")][string] $LogPath = "",
  [Parameter(Mandatory = $false, HelpMessage = "Hardware encoder profile: auto, nvenc, amf, qsv, software")][ValidateSet('auto', 'nvenc', 'amf', 'qsv', 'software')][string] $Encoder = 'auto',
  [Parameter(Mandatory = $false)][switch] $Analyze,
  [Parameter(Mandatory = $false)][switch] $ViewReport,
  [Parameter(Mandatory = $false)][ValidateSet('size-mtime', 'crc32')][string] $HashAlgorithm = 'size-mtime',
  [Parameter(Mandatory = $false)][switch] $Compact
)

$ShowProgress = -not $NoProgress.IsPresent
$script:CanRenderProgress = $ShowProgress
$MoveOnCompletion = -not $SkipMoveOnCompletion.IsPresent
$RefreshArrOnCompletion = -not $SkipArrRefresh.IsPresent
$script:LogEnabled = ($LogEnabled.IsPresent -or $Analyze.IsPresent -or $Compact.IsPresent)
$script:StopRequested = $false
$script:CQPRateControlInt = 0
$script:ShowOutputCmdEnabled = $ShowOutputCmd.IsPresent
$script:LastTextProgressUpdate = [datetime]::MinValue
$script:LastProgressRender = [datetime]::MinValue
$script:LastVerboseSnapshot = [datetime]::MinValue
$script:PreviousProgressView = $null
$script:ScanProcessedCount = 0
$script:LastScannedFile = ""
$script:CancellationSource = $null
$script:CancellationToken = [System.Threading.CancellationToken]::None
$script:EngineExitSubscription = $null
$script:StateTableRef = $null
$script:PendingQueueRef = $null
$script:CandidateQueueRef = $null
$script:TotalWorkCount = 0
$script:FFprobeTimeoutSeconds = 90
$script:HWProfile = $null
$script:AnalyzeTempArtifacts = [System.Collections.Generic.List[string]]::new()
$script:ProcessJobHandle = [IntPtr]::Zero
$script:ProcessJobEnabled = $false
$script:ProcessJobAssignFailed = $false
$script:ContainmentWarningEmitted = $false
$script:ChildWatchdogProcess = $null
$script:TestProbeAnalyzeDuration = '64M'
$script:TestProbeSize = '64M'
$script:TestFrameCount = 48
$script:CudaAv1DecodeSupportKnown = $false
$script:CudaAv1DecodeSupported = $false

if ($Analyze.IsPresent -and (-not $PSBoundParameters.ContainsKey('HashAlgorithm'))) {
  $HashAlgorithm = 'size-mtime'
}

if ($PSVersionTable.PSVersion -lt [version]'7.5.0') {
  throw "This script requires PowerShell 7.5 or newer. Current version: $($PSVersionTable.PSVersion)"
}

function Initialize-Crc32Support {
  [CmdletBinding()]
  param()

  if (([System.Management.Automation.PSTypeName]'ffmpegH265.Crc32Calculator').Type) {
    return
  }

  Add-Type -TypeDefinition @"
using System;
using System.IO;

namespace ffmpegH265 {
  public static class Crc32Calculator {
    private static readonly uint[] Table = CreateTable();

    public static uint Compute(byte[] bytes) {
      if (bytes == null) {
        throw new ArgumentNullException(nameof(bytes));
      }

      return Append(0u, bytes, 0, bytes.Length);
    }

    public static uint ComputeFromFile(string filePath) {
      if (string.IsNullOrWhiteSpace(filePath)) {
        throw new ArgumentException("File path must not be empty.", nameof(filePath));
      }

      using var stream = File.OpenRead(filePath);
      var buffer = new byte[65536];
      uint crc = 0u;
      int read;
      while ((read = stream.Read(buffer, 0, buffer.Length)) > 0) {
        crc = Append(crc, buffer, 0, read);
      }

      return crc;
    }

    private static uint Append(uint currentCrc, byte[] buffer, int offset, int count) {
      var crc = ~currentCrc;
      var end = offset + count;

      for (var index = offset; index < end; index++) {
        crc = (crc >> 8) ^ Table[(crc ^ buffer[index]) & 0xFFu];
      }

      return ~crc;
    }

    private static uint[] CreateTable() {
      const uint polynomial = 0xEDB88320u;
      var table = new uint[256];

      for (uint index = 0; index < table.Length; index++) {
        uint entry = index;
        for (var bit = 0; bit < 8; bit++) {
          if ((entry & 1u) != 0u) {
            entry = (entry >> 1) ^ polynomial;
          }
          else {
            entry >>= 1;
          }
        }

        table[index] = entry;
      }

      return table;
    }
  }
}
"@
}

Initialize-Crc32Support

function Initialize-ChildProcessContainment {
  [CmdletBinding()]
  param()

  if (-not $IsWindows) { return }
  if ($script:ProcessJobEnabled) { return }

  try {
    if (-not ([System.Management.Automation.PSTypeName]'ffmpegH265.JobObjectNative').Type) {
      Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;

namespace ffmpegH265 {
  [Flags]
  public enum JobObjectLimitFlags : uint {
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
  }

  [StructLayout(LayoutKind.Sequential)]
  public struct IO_COUNTERS {
    public ulong ReadOperationCount;
    public ulong WriteOperationCount;
    public ulong OtherOperationCount;
    public ulong ReadTransferCount;
    public ulong WriteTransferCount;
    public ulong OtherTransferCount;
  }

  [StructLayout(LayoutKind.Sequential)]
  public struct JOBOBJECT_BASIC_LIMIT_INFORMATION {
    public long PerProcessUserTimeLimit;
    public long PerJobUserTimeLimit;
    public uint LimitFlags;
    public UIntPtr MinimumWorkingSetSize;
    public UIntPtr MaximumWorkingSetSize;
    public uint ActiveProcessLimit;
    public UIntPtr Affinity;
    public uint PriorityClass;
    public uint SchedulingClass;
  }

  [StructLayout(LayoutKind.Sequential)]
  public struct JOBOBJECT_EXTENDED_LIMIT_INFORMATION {
    public JOBOBJECT_BASIC_LIMIT_INFORMATION BasicLimitInformation;
    public IO_COUNTERS IoInfo;
    public UIntPtr ProcessMemoryLimit;
    public UIntPtr JobMemoryLimit;
    public UIntPtr PeakProcessMemoryUsed;
    public UIntPtr PeakJobMemoryUsed;
  }

  public static class JobObjectNative {
    public const int JobObjectExtendedLimitInformation = 9;

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    public static extern IntPtr CreateJobObject(IntPtr lpJobAttributes, string lpName);

    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern bool SetInformationJobObject(IntPtr hJob, int JobObjectInfoClass, IntPtr lpJobObjectInfo, uint cbJobObjectInfoLength);

    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);

    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern bool CloseHandle(IntPtr hObject);
  }
}
"@ -Language CSharp -ErrorAction Stop
    }

    $jobHandle = [ffmpegH265.JobObjectNative]::CreateJobObject([IntPtr]::Zero, $null)
    if ($jobHandle -eq [IntPtr]::Zero) {
      Write-VerboseParallelLog -Message "Process containment unavailable: CreateJobObject failed (Win32=$([Runtime.InteropServices.Marshal]::GetLastWin32Error()))."
      return
    }

    $limitInfo = [ffmpegH265.JOBOBJECT_EXTENDED_LIMIT_INFORMATION]::new()
    $limitInfo.BasicLimitInformation.LimitFlags = [uint32][ffmpegH265.JobObjectLimitFlags]::JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE

    $size = [Runtime.InteropServices.Marshal]::SizeOf([type][ffmpegH265.JOBOBJECT_EXTENDED_LIMIT_INFORMATION])
    $ptr = [Runtime.InteropServices.Marshal]::AllocHGlobal($size)
    try {
      [Runtime.InteropServices.Marshal]::StructureToPtr($limitInfo, $ptr, $false)
      $ok = [ffmpegH265.JobObjectNative]::SetInformationJobObject($jobHandle, [ffmpegH265.JobObjectNative]::JobObjectExtendedLimitInformation, $ptr, [uint32]$size)
      if (-not $ok) {
        $err = [Runtime.InteropServices.Marshal]::GetLastWin32Error()
        [void][ffmpegH265.JobObjectNative]::CloseHandle($jobHandle)
        Write-VerboseParallelLog -Message "Process containment unavailable: SetInformationJobObject failed (Win32=$err)."
        return
      }
    }
    finally {
      [Runtime.InteropServices.Marshal]::FreeHGlobal($ptr)
    }

    $script:ProcessJobHandle = $jobHandle
    $script:ProcessJobEnabled = $true
    $script:ProcessJobAssignFailed = $false
    Write-VerboseParallelLog -Message "Process containment enabled (KillOnJobClose)."
  }
  catch {
    $script:ProcessJobHandle = [IntPtr]::Zero
    $script:ProcessJobEnabled = $false
    $script:ProcessJobAssignFailed = $true
    Write-VerboseParallelLog -Message "Process containment initialization failed: $($_.Exception.Message)"
  }
}

function Register-ChildProcess {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory = $true)][System.Diagnostics.Process]$Process,
    [Parameter(Mandatory = $false)][int]$JobId = 0
  )

  if (-not $IsWindows) { return }
  if (-not $script:ProcessJobEnabled) { return }
  if ($script:ProcessJobHandle -eq [IntPtr]::Zero) { return }
  if ($script:ProcessJobAssignFailed) { return }

  try {
    $assigned = [ffmpegH265.JobObjectNative]::AssignProcessToJobObject($script:ProcessJobHandle, $Process.Handle)
    if (-not $assigned) {
      $script:ProcessJobAssignFailed = $true
      $err = [Runtime.InteropServices.Marshal]::GetLastWin32Error()
      if (-not $script:ContainmentWarningEmitted) {
        $script:ContainmentWarningEmitted = $true
        Write-ParallelLog -Message "Process containment assignment failed (Win32=$err). Falling back to manual/watchdog cleanup." -Level Warning -Target Both
      }
      Write-VerboseParallelLog -Message "Process containment assignment failed (Win32=$err). Continuing with fallback cleanup." -JobId $JobId
    }
  }
  catch {
    $script:ProcessJobAssignFailed = $true
    if (-not $script:ContainmentWarningEmitted) {
      $script:ContainmentWarningEmitted = $true
      Write-ParallelLog -Message "Process containment assignment failed due to exception. Falling back to manual/watchdog cleanup." -Level Warning -Target Both
    }
    Write-VerboseParallelLog -Message "Process containment assignment exception: $($_.Exception.Message). Continuing with fallback cleanup." -JobId $JobId
  }
}

function Start-ChildProcessWatchdog {
  [CmdletBinding()]
  param()

  if (-not $IsWindows) { return }
  if ($script:ChildWatchdogProcess -and (-not $script:ChildWatchdogProcess.HasExited)) { return }

  try {
    $watchdogScript = @"
`$parentPid = $PID

while (Get-Process -Id `$parentPid -ErrorAction SilentlyContinue) {
  Start-Sleep -Milliseconds 500
}

Start-Sleep -Seconds 1

try {
  `$procTable = @{}
  foreach (`$proc in @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue)) {
    `$procTable[[int]`$proc.ProcessId] = `$proc
  }

  function Test-IsDescendantProcess {
    param(
      [int]`$ProcessId,
      [int]`$AncestorPid,
      [hashtable]`$Table
    )

    `$seen = New-Object 'System.Collections.Generic.HashSet[int]'
    `$current = `$ProcessId
    while (`$Table.ContainsKey(`$current)) {
      if (`$seen.Contains(`$current)) { return `$false }
      [void]`$seen.Add(`$current)

      `$parent = [int]`$Table[`$current].ParentProcessId
      if (`$parent -eq `$AncestorPid) { return `$true }
      if (`$parent -le 0 -or `$parent -eq `$current) { return `$false }

      `$current = `$parent
    }

    return `$false
  }

  foreach (`$proc in @(`$procTable.Values | Where-Object { `$_.Name -ieq 'ffmpeg.exe' -or `$_.Name -ieq 'ffprobe.exe' })) {
    if (Test-IsDescendantProcess -ProcessId ([int]`$proc.ProcessId) -AncestorPid `$parentPid -Table `$procTable) {
      Stop-Process -Id ([int]`$proc.ProcessId) -Force -ErrorAction SilentlyContinue
    }
  }
}
catch { }
"@

    $encoded = [Convert]::ToBase64String([System.Text.Encoding]::Unicode.GetBytes($watchdogScript))
    $pwshCommand = if ($PSVersionTable.PSEdition -ieq 'Core') { 'pwsh' } else { 'powershell' }
    $script:ChildWatchdogProcess = Start-Process -FilePath $pwshCommand -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-EncodedCommand', $encoded) -WindowStyle Hidden -PassThru
    Write-VerboseParallelLog -Message "Child process watchdog started (PID=$($script:ChildWatchdogProcess.Id))."
  }
  catch {
    $script:ChildWatchdogProcess = $null
    Write-VerboseParallelLog -Message "Unable to start child process watchdog: $($_.Exception.Message)"
  }
}

# Snapshot script parameters so compatibility options are explicitly represented and traceable.
$script:RunConfig = [ordered]@{
  Path                 = $Path
  MaxParallelJobs      = $MaxParallelJobs
  ShowOutputCmd        = $ShowOutputCmd.IsPresent
  NoProgress           = $NoProgress.IsPresent
  ResizeResolution     = $ResizeResolution
  ForceResize          = $ForceResize.IsPresent
  RetainAspect         = $RetainAspect.IsPresent
  CanScaleUp           = $CanScaleUp.IsPresent
  CanScaleDown         = $CanScaleDown.IsPresent
  CQPRateControl       = $CQPRateControl
  BitrateControl       = $BitrateControl
  ForceConvert         = $ForceConvert.IsPresent
  CanReprocess         = $CanReprocess.IsPresent
  SkipMoveOnCompletion = $SkipMoveOnCompletion.IsPresent
  SkipArrRefresh       = $SkipArrRefresh.IsPresent
  LogEnabled           = $LogEnabled.IsPresent
  LogVerbose           = $LogVerbose.IsPresent
  ExitOnError          = $ExitOnError.IsPresent
  SortExpression       = $SortExpression
  UserFilterDefined    = ($null -ne $UserFilter)
  AudioLang            = ($AudioLang -join ',')
  SubLang              = ($SubLang -join ',')
  LastRunDate          = $LastRunDate
  SkipFileLock         = $SkipFileLock.IsPresent
  ConfigPath           = $ConfigPath
  Encoder              = $Encoder
  Analyze              = $Analyze.IsPresent
  ViewReport           = $ViewReport.IsPresent
  Compact              = $Compact.IsPresent
  HashAlgorithm        = $HashAlgorithm
}

if (-not [int]::TryParse($CQPRateControl, [ref]$script:CQPRateControlInt)) {
  throw "Invalid CQPRateControl '$CQPRateControl'. Expected an integer value, for example 28."
}

if ($script:CanRenderProgress) {
  try {
    $null = $Host.UI.RawUI

    # Minimal view avoids the extra ASCII bar line in many hosts and keeps rows compact.
    try {
      $script:PreviousProgressView = $PSStyle.Progress.View
      $PSStyle.Progress.View = 'Minimal'
    }
    catch { $script:PreviousProgressView = $null }
  }
  catch {
    $script:CanRenderProgress = $false
    Write-Host "Progress UI is not available in this host; continuing without Write-Progress rendering." -ForegroundColor Yellow
  }
}

if ($MaxParallelJobs -lt 1) { $MaxParallelJobs = 1 }
if ($MaxParallelJobs -gt 4) { $MaxParallelJobs = 4 }

if ($MaxParallelJobs -gt 3) {
  Write-Host "Warning: MaxParallelJobs=$MaxParallelJobs may exceed practical hardware encoder concurrency on many systems." -ForegroundColor Yellow
}

$script:ScriptRoot = $PSScriptRoot
if (-not $script:ScriptRoot) {
  $script:ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
}
# When invoked via a symlink, $PSScriptRoot resolves to the target directory.
# Capture the symlink's own directory so config discovery finds files placed beside it.
$script:InvokedScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Source
$script:InvokedScriptPath = $MyInvocation.MyCommand.Source
# Keep a normalized script path for process ownership checks in queue lock recovery.
if ([string]::IsNullOrWhiteSpace($script:InvokedScriptPath)) {
  $script:InvokedScriptPath = $MyInvocation.MyCommand.Path
}
# Derive config filename from the invoked script name (e.g. foo.ps1 -> foo.config.json).
$invokedScriptName = [System.IO.Path]::GetFileNameWithoutExtension($MyInvocation.MyCommand.Source)
if ([string]::IsNullOrWhiteSpace($invokedScriptName)) {
  $invokedScriptName = [System.IO.Path]::GetFileNameWithoutExtension($MyInvocation.MyCommand.Path)
}
$script:ConfigFileName = "$invokedScriptName.config.json"
$script:ConfigFileNames = [System.Collections.Generic.List[string]]::new()
[void]$script:ConfigFileNames.Add($script:ConfigFileName)
$targetScriptName = [System.IO.Path]::GetFileNameWithoutExtension($MyInvocation.MyCommand.Path)
if (-not [string]::IsNullOrWhiteSpace($targetScriptName)) {
  $targetConfigName = "$targetScriptName.config.json"
  if (-not $script:ConfigFileNames.Contains($targetConfigName)) {
    [void]$script:ConfigFileNames.Add($targetConfigName)
  }
}
# When launched from a wrapper script (e.g. ConvertMovieLibrary_264.ps1), capture its directory.
$script:CallerScriptDir = $null
$callerFrame = Get-PSCallStack | Select-Object -Skip 1 -First 1
if ($callerFrame -and -not [string]::IsNullOrWhiteSpace($callerFrame.ScriptName)) {
  $script:CallerScriptDir = Split-Path -Parent $callerFrame.ScriptName
}

$script:LogPrefix = $invokedScriptName
$script:logFileName = "${script:LogPrefix}_$(Get-Date -Format "yyyyMMddHHmmssffff")_$($PID).log"
$script:logFilePath = $null
$script:LogMutexName = if ($IsWindows) { "Global\${script:LogPrefix}_log" }
else { "${script:LogPrefix}_log" }
$script:LogMutex = $null
$script:QueuedFileMutexes = @{}
$script:ArrRefreshJobs = [System.Collections.Generic.List[object]]::new()
$script:ArrRefreshJobMeta = @{}
$script:ArrRefreshRequestedKeys = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
$script:ArrRefreshTargets = @{}

# Configuration values are required, resolved as env var first, then config file.
# Precedence for environment/deployment settings is:
# 1) Environment variable
# 2) Config file value
# 3) Fail fast (for required keys)
function Get-ConfigValue {
  param(
    [Parameter(Mandatory = $true)][string]$ConfigKey,
    [Parameter(Mandatory = $true)][string]$EnvVarName,
    [Parameter(Mandatory = $false)][object]$ConfigData = $null
  )

  $value = [Environment]::GetEnvironmentVariable($EnvVarName)

  if ([string]::IsNullOrWhiteSpace([string]$value) -and $ConfigData -and $ConfigData.PSObject.Properties[$ConfigKey]) {
    $value = $ConfigData.$ConfigKey
  }

  if ([string]::IsNullOrWhiteSpace([string]$value)) {
    $configHints = ($script:ConfigFileNames | ForEach-Object { "'$_'" }) -join ', '
    throw "Required setting '$ConfigKey' is missing. Set '$ConfigKey' in one of: $configHints, or environment variable '$EnvVarName'."
  }

  return $value
}

function Get-OptionalConfigValue {
  param(
    [Parameter(Mandatory = $true)][string]$ConfigKey,
    [Parameter(Mandatory = $true)][string]$EnvVarName,
    [Parameter(Mandatory = $false)][object]$ConfigData = $null
  )

  $value = [Environment]::GetEnvironmentVariable($EnvVarName)

  if ([string]::IsNullOrWhiteSpace([string]$value) -and $ConfigData -and $ConfigData.PSObject.Properties[$ConfigKey]) {
    $value = $ConfigData.$ConfigKey
  }

  return $value
}

function Resolve-LogPath {
  param(
    [Parameter(Mandatory = $false)][string]$ExplicitPath,
    [Parameter(Mandatory = $true)][string]$DefaultPrefix,
    [Parameter(Mandatory = $true)][string]$DefaultFileName,
    [Parameter(Mandatory = $true)][string[]]$SearchLocations,
    [Parameter(Mandatory = $false)][string]$LoadedConfigDirectory
  )

  if (-not [string]::IsNullOrWhiteSpace($ExplicitPath)) {
    if ([System.IO.Path]::HasExtension($ExplicitPath) -and
      -not (Test-Path -LiteralPath $ExplicitPath -PathType Container -ErrorAction SilentlyContinue)) {
      # Full file path (e.g. C:\logs\my_log.log) — use as the log file directly.
      $prefix = [System.IO.Path]::GetFileNameWithoutExtension($ExplicitPath)
      return @{
        Directory = Split-Path -Parent $ExplicitPath
        FileName  = Split-Path -Leaf $ExplicitPath
        FilePath  = $ExplicitPath
        Prefix    = $prefix
      }
    }
    # Existing or new directory — use invocation-derived log name within it.
    return @{
      Directory = $ExplicitPath
      FileName  = $DefaultFileName
      FilePath  = Join-Path $ExplicitPath $DefaultFileName
      Prefix    = $DefaultPrefix
    }
  }

  # Auto-discover: config dir, search locations, or $PWD.
  $dir = $PWD.Path
  if (-not [string]::IsNullOrWhiteSpace($LoadedConfigDirectory)) {
    $dir = $LoadedConfigDirectory
  }
  else {
    foreach ($location in $SearchLocations) {
      if ([string]::IsNullOrWhiteSpace($location)) { continue }
      if (Test-Path -LiteralPath $location -PathType Container -ErrorAction SilentlyContinue) {
        $dir = $location
        break
      }
    }
  }
  return @{
    Directory = $dir
    FileName  = $DefaultFileName
    FilePath  = Join-Path $dir $DefaultFileName
    Prefix    = $DefaultPrefix
  }
}

function Get-Crc32HexFromBytes {
  param(
    [Parameter(Mandatory = $true)][byte[]]$Bytes
  )

  return ('{0:x8}' -f [ffmpegH265.Crc32Calculator]::Compute($Bytes))
}

function Get-Crc32HexFromFile {
  param(
    [Parameter(Mandatory = $true)][string]$FilePath
  )

  return ('{0:x8}' -f [ffmpegH265.Crc32Calculator]::ComputeFromFile($FilePath))
}

function Get-QueueLockName {
  param(
    [Parameter(Mandatory = $true)][string]$FilePath
  )

  $hash = Get-QueueLockHashKey -FilePath $FilePath

  if ($IsWindows) {
    return "Global\ffmpeg_h265_queue_$hash"
  }
  return "ffmpeg_h265_queue_$hash"
}

function Get-QueueLockFilePath {
  param(
    [Parameter(Mandatory = $true)][string]$FilePath,
    [Parameter(Mandatory = $true)][string]$ProcessingRoot
  )

  $hash = Get-QueueLockHashKey -FilePath $FilePath

  return (Join-Path $ProcessingRoot (".ffmpeg_h265_queue_lock_{0}.json" -f $hash))
}

function Get-QueueLockHashKey {
  param(
    [Parameter(Mandatory = $true)][string]$FilePath
  )

  $normalized = [System.IO.Path]::GetFullPath($FilePath)
  if ($IsWindows) { $normalized = $normalized.ToLowerInvariant() }

  $bytes = [System.Text.Encoding]::UTF8.GetBytes($normalized)
  $crcHex = Get-Crc32HexFromBytes -Bytes $bytes

  # Keep names short while reducing accidental collisions beyond raw CRC32.
  $lengthHex = '{0:x8}' -f $bytes.Length
  return "$crcHex$lengthHex"
}

function Register-QueuedFileMutex {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory = $true)][string]$FilePath,
    [Parameter(Mandatory = $true)][string]$ProcessingRoot
  )

  $lockName = Get-QueueLockName -FilePath $FilePath
  $lockFilePath = Get-QueueLockFilePath -FilePath $FilePath -ProcessingRoot $ProcessingRoot
  $mutex = [System.Threading.Mutex]::new($false, $lockName)
  $acquired = $false

  try {
    try {
      $acquired = $mutex.WaitOne(0)
    }
    catch [System.Threading.AbandonedMutexException] {
      $acquired = $true
    }

    if (-not $acquired) {
      try { $mutex.Dispose() } catch { }
      return $null
    }

    $metadata = [ordered]@{
      pid             = $PID
      owner_start_utc = $null
      script_path     = $script:InvokedScriptPath
      script_name     = [System.IO.Path]::GetFileName($script:InvokedScriptPath)
      file_path       = $FilePath
      lock_name       = $lockName
      acquired_utc    = (Get-Date).ToUniversalTime().ToString('o')
    }

    try {
      $ownerProc = Get-Process -Id $PID -ErrorAction SilentlyContinue
      if ($ownerProc -and $ownerProc.StartTime) {
        $metadata.owner_start_utc = $ownerProc.StartTime.ToUniversalTime().ToString('o')
      }
    }
    catch {
      $metadata.owner_start_utc = $null
    }

    try {
      $metadata | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $lockFilePath -Encoding UTF8
    }
    catch {
      Write-VerboseParallelLog -Message "Unable to write queue lock metadata '$lockFilePath': $($_.Exception.Message)"
    }

    return [PSCustomObject]@{
      Mutex    = $mutex
      LockName = $lockName
      LockFile = $lockFilePath
    }
  }
  catch {
    if ($acquired) {
      try { [void]$mutex.ReleaseMutex() } catch { }
    }
    try { $mutex.Dispose() } catch { }
    throw
  }
}

function Unregister-QueuedFileMutex {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory = $true)][string]$FilePath
  )

  if (-not $script:QueuedFileMutexes.ContainsKey($FilePath)) { return }

  $entry = $script:QueuedFileMutexes[$FilePath]
  if (-not $entry) {
    [void]$script:QueuedFileMutexes.Remove($FilePath)
    return
  }

  if ($entry.LockFile -and (Test-Path -LiteralPath $entry.LockFile -PathType Leaf -ErrorAction SilentlyContinue)) {
    try { Remove-Item -LiteralPath $entry.LockFile -Force -ErrorAction SilentlyContinue }
    catch {
      Write-VerboseParallelLog -Message "Unable to remove queue lock metadata '$($entry.LockFile)': $($_.Exception.Message)"
    }
  }

  if ($entry.Mutex) {
    try { [void]$entry.Mutex.ReleaseMutex() } catch { }
    try { $entry.Mutex.Dispose() } catch { }
  }

  [void]$script:QueuedFileMutexes.Remove($FilePath)
}

function Test-QueuedFileMutexHasActiveOwner {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory = $true)][string]$LockName
  )

  if ([string]::IsNullOrWhiteSpace($LockName)) { return $false }

  $mutex = $null
  $acquired = $false
  try {
    $mutex = [System.Threading.Mutex]::new($false, $LockName)
    try {
      $acquired = $mutex.WaitOne(0)
    }
    catch [System.Threading.AbandonedMutexException] {
      $acquired = $true
    }

    return (-not $acquired)
  }
  finally {
    if ($mutex) {
      if ($acquired) {
        try { [void]$mutex.ReleaseMutex() } catch { }
      }
      try { $mutex.Dispose() } catch { }
    }
  }
}

function Remove-StaleQueuedFileLockMetadata {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory = $true)][string]$ProcessingRoot
  )

  if ([string]::IsNullOrWhiteSpace($ProcessingRoot)) { return }
  if (-not (Test-Path -LiteralPath $ProcessingRoot -PathType Container -ErrorAction SilentlyContinue)) { return }

  $pattern = Join-Path $ProcessingRoot '.ffmpeg_h265_queue_lock_*.json'
  $staleFiles = @(Get-ChildItem -Path $pattern -File -ErrorAction SilentlyContinue)
  if ($staleFiles.Count -eq 0) { return }

  $ownerProcessCache = @{}
  $removed = 0
  foreach ($staleFile in $staleFiles) {
    try {
      $raw = Get-Content -LiteralPath $staleFile.FullName -Raw -ErrorAction Stop
      $meta = $raw | ConvertFrom-Json -ErrorAction Stop

      $lockName = [string]$meta.lock_name
      if (-not [string]::IsNullOrWhiteSpace($lockName)) {
        if (Test-QueuedFileMutexHasActiveOwner -LockName $lockName) {
          continue
        }

        Remove-Item -LiteralPath $staleFile.FullName -Force -ErrorAction SilentlyContinue
        $removed++
        continue
      }

      $ownerPid = 0
      [void][int]::TryParse("$($meta.pid)", [ref]$ownerPid)

      if ($ownerPid -le 0) {
        Remove-Item -LiteralPath $staleFile.FullName -Force -ErrorAction SilentlyContinue
        $removed++
        continue
      }

      if (-not $ownerProcessCache.ContainsKey($ownerPid)) {
        $ownerProcessCache[$ownerPid] = Get-Process -Id $ownerPid -ErrorAction SilentlyContinue
      }

      $ownerProc = $ownerProcessCache[$ownerPid]
      if (-not $ownerProc) {
        Remove-Item -LiteralPath $staleFile.FullName -Force -ErrorAction SilentlyContinue
        $removed++
        continue
      }

      $ownerIsExpected = $true

      # If PID has been reused by a different process start instance, treat as stale.
      $recordedStartUtc = [string]$meta.owner_start_utc
      if (-not [string]::IsNullOrWhiteSpace($recordedStartUtc) -and $ownerProc.StartTime) {
        try {
          $recordedStart = [datetime]::Parse($recordedStartUtc).ToUniversalTime()
          $activeStart = $ownerProc.StartTime.ToUniversalTime()
          $startDeltaSeconds = [Math]::Abs(($activeStart - $recordedStart).TotalSeconds)
          if ($startDeltaSeconds -gt 5.0) {
            $ownerIsExpected = $false
          }
        }
        catch {
          $ownerIsExpected = $false
        }
      }

      if (-not $ownerIsExpected) {
        Remove-Item -LiteralPath $staleFile.FullName -Force -ErrorAction SilentlyContinue
        $removed++
      }
    }
    catch {
      # Corrupt lock metadata should not block startup; remove it.
      try {
        Remove-Item -LiteralPath $staleFile.FullName -Force -ErrorAction SilentlyContinue
        $removed++
      }
      catch {
        Write-VerboseParallelLog -Message "Unable to remove stale queue lock metadata '$($staleFile.FullName)': $($_.Exception.Message)"
      }
    }
  }

  if ($removed -gt 0) {
    Write-ParallelLog -Message "Removed $removed stale queue lock metadata file(s) from '$ProcessingRoot'." -Target Both
  }
}

function Unregister-QueuedFileMutexes {
  [CmdletBinding()]
  param()

  foreach ($key in @($script:QueuedFileMutexes.Keys)) {
    Unregister-QueuedFileMutex -FilePath $key
  }

  $script:QueuedFileMutexes.Clear()
}

function Resolve-Config {
  param(
    [Parameter(Mandatory = $false)][string]$ExplicitConfigPath,
    [Parameter(Mandatory = $true)][string[]]$CandidateConfigFileNames,
    [Parameter(Mandatory = $true)][string[]]$AutoDiscoveryLocations
  )

  $resolvedConfigData = $null
  $resolvedConfigLocations = @()
  $resolvedLoadedConfigDirectory = $null
  $resolvedConfigFileName = $CandidateConfigFileNames[0]
  $resolvedConfigFileNames = [System.Collections.Generic.List[string]]::new()
  foreach ($candidateConfigFileName in $CandidateConfigFileNames) {
    if (-not [string]::IsNullOrWhiteSpace($candidateConfigFileName) -and -not $resolvedConfigFileNames.Contains($candidateConfigFileName)) {
      [void]$resolvedConfigFileNames.Add($candidateConfigFileName)
    }
  }

  if ([string]::IsNullOrWhiteSpace($ExplicitConfigPath)) {
    $envConfigPath = [Environment]::GetEnvironmentVariable('FFENC_CONFIGPATH')
    if (-not [string]::IsNullOrWhiteSpace($envConfigPath)) {
      $ExplicitConfigPath = $envConfigPath
    }
  }

  if (-not [string]::IsNullOrWhiteSpace($ExplicitConfigPath)) {
    if ($ExplicitConfigPath -match '\.json$' -and (Test-Path -LiteralPath $ExplicitConfigPath -PathType Leaf)) {
      try {
        $resolvedConfigData = Get-Content -LiteralPath $ExplicitConfigPath -Raw | ConvertFrom-Json
        $resolvedLoadedConfigDirectory = Split-Path -Parent $ExplicitConfigPath
        $resolvedConfigFileName = Split-Path -Leaf $ExplicitConfigPath
        $resolvedConfigFileNames.Clear()
        [void]$resolvedConfigFileNames.Add($resolvedConfigFileName)
      }
      catch {
        Write-Warning "Failed to load config from '$ExplicitConfigPath': $($_.Exception.Message)"
      }
      $resolvedConfigLocations = @($resolvedLoadedConfigDirectory)
    }
    else {
      $resolvedConfigLocations = @($ExplicitConfigPath)
    }
  }
  else {
    $seenConfigLocations = [System.Collections.Generic.HashSet[string]]::new(
      $(if ($IsWindows) { [System.StringComparer]::OrdinalIgnoreCase } else { [System.StringComparer]::Ordinal })
    )
    foreach ($candidateLocation in $AutoDiscoveryLocations) {
      if ([string]::IsNullOrWhiteSpace($candidateLocation)) { continue }
      if ($seenConfigLocations.Add($candidateLocation)) {
        $resolvedConfigLocations += $candidateLocation
      }
    }
  }

  if (-not $resolvedConfigData) {
    foreach ($location in $resolvedConfigLocations) {
      if ([string]::IsNullOrWhiteSpace($location)) { continue }

      foreach ($candidateConfigName in $resolvedConfigFileNames) {
        $configPath = Join-Path $location $candidateConfigName
        if (Test-Path -LiteralPath $configPath) {
          try {
            $resolvedConfigData = Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json
            $resolvedLoadedConfigDirectory = Split-Path -Parent $configPath
            $resolvedConfigFileName = $candidateConfigName
            break
          }
          catch {
            Write-Warning "Failed to load config from '$configPath': $($_.Exception.Message)"
          }
        }
      }

      if ($resolvedConfigData) { break }
    }
  }

  $searchedPaths = @(
    foreach ($location in ($resolvedConfigLocations | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })) {
      foreach ($candidateConfigName in $resolvedConfigFileNames) {
        Join-Path $location $candidateConfigName
      }
    }
  )

  return [PSCustomObject]@{
    ConfigData            = $resolvedConfigData
    ConfigLocations       = $resolvedConfigLocations
    LoadedConfigDirectory = $resolvedLoadedConfigDirectory
    ConfigFileName        = $resolvedConfigFileName
    ConfigFileNames       = @($resolvedConfigFileNames)
    SearchedPaths         = $searchedPaths
  }
}

# Standalone -ViewReport: no -Path and no config file needed — launch viewer and exit immediately.
if ($ViewReport -and -not $Analyze -and -not $Compact) {
  $serveScriptPath = Join-Path $PSScriptRoot 'serve_report.ps1'
  if (-not (Test-Path -LiteralPath $serveScriptPath -PathType Leaf -ErrorAction SilentlyContinue)) {
    Write-Error "Cannot open report viewer because '$serveScriptPath' was not found."
    exit 1
  }
  $pwshCommand = if ($PSVersionTable.PSEdition -ieq 'Core') { 'pwsh' } else { 'powershell' }
  $argList = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $serveScriptPath, '-Report', 'runtime')
  try {
    Start-Process -FilePath $pwshCommand -ArgumentList $argList -WorkingDirectory $PSScriptRoot | Out-Null
    Write-Host "Started report server helper using '$serveScriptPath'." -ForegroundColor Cyan
  }
  catch {
    Write-Error "Failed to start report server helper '$serveScriptPath': $($_.Exception.Message)"
    exit 1
  }
  exit 0
}

$autoDiscoveryLocations = @(
  $PWD.Path,
  $script:CallerScriptDir,
  $script:InvokedScriptDir,
  $script:ScriptRoot
) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }

if (-not $autoDiscoveryLocations -or $autoDiscoveryLocations.Count -eq 0) {
  $autoDiscoveryLocations = @($PWD.Path)
}

$resolvedConfig = Resolve-Config -ExplicitConfigPath $ConfigPath -CandidateConfigFileNames @($script:ConfigFileNames) -AutoDiscoveryLocations $autoDiscoveryLocations

$configData = $resolvedConfig.ConfigData
$configLocations = @($resolvedConfig.ConfigLocations)
$loadedConfigDirectory = $resolvedConfig.LoadedConfigDirectory
$script:ConfigFileName = $resolvedConfig.ConfigFileName
$script:ConfigFileNames.Clear()
foreach ($resolvedConfigFileName in @($resolvedConfig.ConfigFileNames)) {
  [void]$script:ConfigFileNames.Add($resolvedConfigFileName)
}

if (-not $configData) {
  Write-Host "Configuration file not found. Searched:" -ForegroundColor Yellow
  foreach ($searchedPath in @($resolvedConfig.SearchedPaths)) {
    Write-Host "  $searchedPath" -ForegroundColor Yellow
  }
  Write-Host "Provide -ConfigPath, set FFENC_CONFIGPATH, or place a config file beside the invoked script." -ForegroundColor Yellow
  exit 1
}

$script:ffmpeg_path = Get-ConfigValue -ConfigKey "ffmpeg_path" -EnvVarName "FFENC_FFMPEG_PATH" -ConfigData $configData
$explicitLogPath = Get-OptionalConfigValue -ConfigKey "log_path" -EnvVarName "FFENC_LOG_PATH" -ConfigData $configData
if ($PSBoundParameters.ContainsKey('LogPath') -and -not [string]::IsNullOrWhiteSpace($LogPath)) {
  $explicitLogPath = $LogPath
}
$defaultLogPrefix = $script:LogPrefix
$defaultLogFileName = $script:logFileName
$logResolved = Resolve-LogPath -ExplicitPath $explicitLogPath -DefaultPrefix $defaultLogPrefix -DefaultFileName $defaultLogFileName `
  -SearchLocations $configLocations -LoadedConfigDirectory $loadedConfigDirectory
$script:log_path = $logResolved.Directory
$script:logFileName = $logResolved.FileName
$script:logFilePath = $logResolved.FilePath
$script:LogPrefix = $logResolved.Prefix
$script:LogMutexName = if ($IsWindows) { "Global\${script:LogPrefix}_log" } else { "${script:LogPrefix}_log" }
$processed_path = Get-ConfigValue -ConfigKey "processed_path" -EnvVarName "FFENC_PROCESSED_PATH" -ConfigData $configData
$processing_path = Get-ConfigValue -ConfigKey "processing_path" -EnvVarName "FFENC_PROCESSING_PATH" -ConfigData $configData
$media_path = Get-ConfigValue -ConfigKey "media_path" -EnvVarName "FFENC_MEDIA_PATH" -ConfigData $configData
$moviesSubfolder = Get-OptionalConfigValue -ConfigKey "movies_subfolder" -EnvVarName "FFENC_MOVIES_SUBFOLDER" -ConfigData $configData
$tvShowsSubfolder = Get-OptionalConfigValue -ConfigKey "tv_shows_subfolder" -EnvVarName "FFENC_TV_SHOWS_SUBFOLDER" -ConfigData $configData
if ([string]::IsNullOrWhiteSpace([string]$moviesSubfolder)) { $moviesSubfolder = "Movies" }
if ([string]::IsNullOrWhiteSpace([string]$tvShowsSubfolder)) { $tvShowsSubfolder = "TV Shows" }
$tv_shows_path = Join-Path $media_path $tvShowsSubfolder
$movies_path = Join-Path $media_path $moviesSubfolder
$radarr_baseUri = Get-ConfigValue -ConfigKey "radarr_baseUri" -EnvVarName "RADARR_BASE_URI" -ConfigData $configData
$radarr_apiKey = Get-ConfigValue -ConfigKey "radarr_apiKey" -EnvVarName "RADARR_API_KEY" -ConfigData $configData
$sonarr_baseUri = Get-ConfigValue -ConfigKey "sonarr_baseUri" -EnvVarName "SONARR_BASE_URI" -ConfigData $configData
$sonarr_apiKey = Get-ConfigValue -ConfigKey "sonarr_apiKey" -EnvVarName "SONARR_API_KEY" -ConfigData $configData
$script:ArrRefreshTimeoutSeconds = Get-OptionalConfigValue -ConfigKey "arr_refresh_timeout_seconds" -EnvVarName "FFENC_ARR_REFRESH_TIMEOUT_SECONDS" -ConfigData $configData
if ([string]::IsNullOrWhiteSpace([string]$script:ArrRefreshTimeoutSeconds) -or -not [int]::TryParse($script:ArrRefreshTimeoutSeconds, [ref]$null)) {
  $script:ArrRefreshTimeoutSeconds = 15
}
else {
  $script:ArrRefreshTimeoutSeconds = [int]$script:ArrRefreshTimeoutSeconds
}

$script:NullDevice = if ($IsWindows) { 'NUL' } else { '/dev/null' }
$ffmpegExeName = if ($IsWindows) { 'ffmpeg.exe' } else { 'ffmpeg' }
$ffprobeExeName = if ($IsWindows) { 'ffprobe.exe' } else { 'ffprobe' }

if ((Split-Path -Path $script:ffmpeg_path -Leaf) -ieq $ffmpegExeName) {
  $script:ffmpeg_exe = $script:ffmpeg_path
}
else { $script:ffmpeg_exe = Join-Path $script:ffmpeg_path $ffmpegExeName }

if ((Split-Path -Path $script:ffmpeg_path -Leaf) -ieq $ffprobeExeName) {
  $script:ffprobe_exe = $script:ffmpeg_path
}
elseif ((Split-Path -Path $script:ffmpeg_path -Leaf) -ieq $ffmpegExeName) {
  $script:ffprobe_exe = Join-Path (Split-Path -Parent $script:ffmpeg_path) $ffprobeExeName
}
else { $script:ffprobe_exe = Join-Path $script:ffmpeg_path $ffprobeExeName }

if (-not (Test-Path -LiteralPath $script:ffmpeg_exe)) {
  throw "ffmpeg executable was not found at '$($script:ffmpeg_exe)'. Set ffmpeg_path (or FFENC_FFMPEG_PATH) to the ffmpeg directory or full executable path."
}

if (-not (Test-Path -LiteralPath $script:ffprobe_exe)) {
  throw "ffprobe executable was not found at '$($script:ffprobe_exe)'. Set ffmpeg_path (or FFENC_FFMPEG_PATH) to the ffmpeg directory or full executable path."
}

if (-not (Test-Path -LiteralPath $script:log_path)) {
  try {
    New-Item -Path $script:log_path -ItemType Directory -Force -ErrorAction Stop | Out-Null
  }
  catch {
    $requestedLogPath = $script:log_path
    $fallbackLogPath = $script:InvokedScriptDir
    if ([string]::IsNullOrWhiteSpace($fallbackLogPath) -or
      -not (Test-Path -LiteralPath $fallbackLogPath -PathType Container -ErrorAction SilentlyContinue)) {
      $fallbackLogPath = $script:ScriptRoot
    }
    if ([string]::IsNullOrWhiteSpace($fallbackLogPath) -or
      -not (Test-Path -LiteralPath $fallbackLogPath -PathType Container -ErrorAction SilentlyContinue)) {
      $fallbackLogPath = $PWD.Path
    }

    $script:log_path = $fallbackLogPath
    $script:LogPrefix = $defaultLogPrefix
    $script:logFileName = $defaultLogFileName
    $script:logFilePath = Join-Path $script:log_path $script:logFileName
    $script:LogMutexName = if ($IsWindows) { "Global\${script:LogPrefix}_log" } else { "${script:LogPrefix}_log" }

    Write-Host "Log path fallback active: requested '$requestedLogPath' was unavailable; using '$($script:log_path)'." -ForegroundColor Yellow
    Write-Warning "Failed to create log directory '$requestedLogPath' ($($_.Exception.Message)). Falling back to '$($script:log_path)'."
  }
}

enum LogLevel {
  Information = 0
  Warning = 1
  Error = 2
}

enum OutputTarget {
  None = 0
  Log = 1
  Console = 2
  Both = 3
}

enum ArrType {
  movie
  series
}

enum ResTypes {
  UNKNOWN = 0
  SD = 1
  DVD = 2
  HD = 3
  FHD = 4
  QHD = 5
  UHD_4K = 6
  FUHD_8K = 7
}

function Test-CudaAv1DecodeSupport {
  [CmdletBinding()]
  [OutputType([bool])]
  param(
    [Parameter(Mandatory = $true)][string]$InputPath,
    [Parameter(Mandatory = $false)][int]$JobId = 0
  )

  if ([string]::IsNullOrWhiteSpace($InputPath) -or -not (Test-Path -LiteralPath $InputPath -PathType Leaf -ErrorAction SilentlyContinue)) {
    Write-VerboseParallelLog -Message "CUDA AV1 probe skipped: input path is unavailable ('$InputPath')." -JobId $JobId
    return $false
  }

  $probeArgs = "-hide_banner -v error -hwaccel cuda -hwaccel_output_format cuda -analyzeduration 8M -probesize 8M -i `"$InputPath`" -map 0:v:0 -frames:v 2 -f null $($script:NullDevice)"
  Write-VerboseParallelLog -Message "Running one-time CUDA AV1 decode capability probe: $probeArgs" -JobId $JobId

  $p = New-Object System.Diagnostics.Process
  $p.StartInfo.FileName = $script:ffmpeg_exe
  $p.StartInfo.Arguments = $probeArgs
  $p.StartInfo.UseShellExecute = $false
  $p.StartInfo.RedirectStandardOutput = $false
  $p.StartInfo.RedirectStandardError = $true
  $p.StartInfo.CreateNoWindow = $true

  try {
    $p.Start() | Out-Null
    $stderrTask = $p.StandardError.ReadToEndAsync()

    if (-not $p.WaitForExit(15000)) {
      try { $p.Kill() } catch { }
      try { $p.WaitForExit(2000) } catch { }
      Write-ParallelLog -Message "CUDA AV1 decode capability probe timed out; treating as unsupported." -Level Warning -Target Both -JobId $JobId
      return $false
    }

    if ($p.ExitCode -eq 0) {
      Write-ParallelLog -Message "CUDA AV1 decode capability probe succeeded; AV1 tests may keep CUDA decode variants." -Target Both -JobId $JobId
      return $true
    }

    $err = ""
    try { $err = $stderrTask.GetAwaiter().GetResult().Trim() }
    catch { $err = "" }
    $firstLine = if ([string]::IsNullOrWhiteSpace($err)) { 'Unknown ffmpeg error' }
    else { ($err -split "`r?`n" | Select-Object -First 1).Trim() }

    Write-ParallelLog -Message "CUDA AV1 decode capability probe failed; AV1 tests will skip CUDA decode variants. Reason: $firstLine" -Level Warning -Target Both -JobId $JobId
    return $false
  }
  catch {
    Write-ParallelLog -Message "CUDA AV1 decode capability probe exception; treating as unsupported. $($_.Exception.Message)" -Level Warning -Target Both -JobId $JobId
    return $false
  }
  finally {
    try { $p.Dispose() } catch { }
  }
}

function Write-ParallelLog {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory = $true)][string]$Message,
    [Parameter(Mandatory = $false)][LogLevel]$Level = [LogLevel]::Information,
    [Parameter(Mandatory = $false)][OutputTarget]$Target = [OutputTarget]::Both,
    [Parameter(Mandatory = $false)][int]$JobId = 0
  )

  if (-not $script:LogMutex) {
    $script:LogMutex = [System.Threading.Mutex]::new($false, $script:LogMutexName)
  }

  $effectiveTarget = $Target
  if ((-not $script:LogEnabled) -and ($effectiveTarget -band [OutputTarget]::Log)) {
    $effectiveTarget = [OutputTarget]($effectiveTarget -bxor [OutputTarget]::Log)
  }

  $jobPrefix = ""
  if ($JobId -gt 0) { $jobPrefix = "[Job-$JobId] " }
  $timestamp = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss.fff')"
  $full = "$timestamp - $jobPrefix$Message"

  if ($Level -eq [LogLevel]::Warning) { $full = "$timestamp - [WARNING] $jobPrefix$Message" }
  if ($Level -eq [LogLevel]::Error) { $full = "$timestamp - [ERROR] $jobPrefix$Message" }

  if ($effectiveTarget -band [OutputTarget]::Log) {
    $lockAcquired = $false
    try {
      try { $lockAcquired = $script:LogMutex.WaitOne(5000) }
      catch [System.Threading.AbandonedMutexException] { $lockAcquired = $true }

      if (-not $lockAcquired) {
        throw "Timed out waiting for log mutex '$($script:LogMutexName)'"
      }

      Add-Content -LiteralPath $script:logFilePath -Value $full -ErrorAction Stop
    }
    catch {
      if (-not ($effectiveTarget -band [OutputTarget]::Console)) { throw }
    }
    finally { if ($lockAcquired) { [void]$script:LogMutex.ReleaseMutex() } }
  }

  if ($effectiveTarget -band [OutputTarget]::Console) {
    switch ($Level) {
      ([LogLevel]::Warning) { Write-Host $full -ForegroundColor Yellow }
      ([LogLevel]::Error) { Write-Host $full -ForegroundColor Red }
      default { Write-Host $full }
    }
  }
}

function Write-VerboseParallelLog {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory = $true)][string]$Message,
    [Parameter(Mandatory = $false)][int]$JobId = 0
  )

  if (-not $LogVerbose) { return }

  Write-ParallelLog -Message "[VERBOSE] $Message" -Target Log -JobId $JobId
}

if ($loadedConfigDirectory) {
  $configFile = Join-Path $loadedConfigDirectory $script:ConfigFileName
  Write-ParallelLog -Message "Loaded configuration from: '$configFile'" -Target Both

  if ($configData) {
    $configSummary = ($configData.PSObject.Properties | ForEach-Object {
        $k = $_.Name
        $v = [string]$_.Value
        if ($k -imatch 'key|secret|token|password') {
          if ([string]::IsNullOrWhiteSpace($v)) { $v = '(not set)' }
          elseif (-not $LogVerbose) { $v = ($v.Substring(0, [Math]::Min(4, $v.Length))) + '****' }
        }
        "$k=$v"
      }) -join '; '
    Write-ParallelLog -Message "Configuration settings: $configSummary" -Target Log
  }
}
else {
  $searchedPaths = @(
    foreach ($location in ($configLocations | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })) {
      foreach ($candidateConfigName in $script:ConfigFileNames) {
        Join-Path $location $candidateConfigName
      }
    }
  ) -join ', '
  Write-ParallelLog -Message "No configuration file found. Searched: $searchedPaths" -Level Warning -Target Both
}

Write-ParallelLog -Message "Resolved log output path: '$($script:logFilePath)' (LogEnabled=$script:LogEnabled)." -Target Both

function Read-FFmpegProgressLine {
  [CmdletBinding()]
  [OutputType([PSCustomObject])]
  param(
    [Parameter(Mandatory = $true)][string]$Line,
    [Parameter(Mandatory = $true)][hashtable]$ProgressState
  )

  if ([string]::IsNullOrWhiteSpace($Line) -or (-not $Line.Contains("="))) { return $null }

  $kv = $Line.Split("=", 2)
  $key = $kv[0].Trim()
  $value = $kv[1].Trim()
  $ProgressState[$key] = $value

  if ($key -ieq "progress") {
    $update = [PSCustomObject]($ProgressState.Clone())
    $ProgressState.Clear()
    return $update
  }

  return $null
}

function Get-FFprobeJson {
  [OutputType([PSCustomObject])]
  param(
    [Parameter(Mandatory = $true)][string]$InputPath,
    [Parameter(Mandatory = $false)][switch]$AnalyzeMode
  )

  if ($script:StopRequested -or $script:CancellationToken.IsCancellationRequested) { return $null }

  $ffprobeExe = $script:ffprobe_exe
  $ffprobeArgs = "`"$InputPath`" -v quiet -hide_banner -analyzeduration 4GB -probesize 4GB -show_format -show_streams -print_format json -sexagesimal"
  if ($AnalyzeMode) {
    # Analysis mode only needs metadata; lighter probe settings reduce per-file latency.
    $ffprobeArgs = "`"$InputPath`" -v quiet -hide_banner -analyzeduration 200M -probesize 50M -show_format -show_streams -print_format json"
  }

  $probeProcess = New-Object System.Diagnostics.Process
  $probeProcess.StartInfo.FileName = $ffprobeExe
  $probeProcess.StartInfo.Arguments = $ffprobeArgs
  $probeProcess.StartInfo.UseShellExecute = $false
  $probeProcess.StartInfo.RedirectStandardOutput = $true
  $probeProcess.StartInfo.RedirectStandardError = $true
  $probeProcess.StartInfo.CreateNoWindow = $true

  $started = $probeProcess.Start()
  if (-not $started) { return $null }

  $stdoutTask = $probeProcess.StandardOutput.ReadToEndAsync()
  $stderrTask = $probeProcess.StandardError.ReadToEndAsync()
  $probeStartedAt = Get-Date
  $lastProbeUiTick = [datetime]::MinValue

  while (-not $probeProcess.HasExited) {
    # Keep active encode progress fresh even when a preflight probe is waiting.
    if ($script:StateTableRef -and ($script:TotalWorkCount -gt 0)) {
      $now = Get-Date
      if (($now - $lastProbeUiTick).TotalMilliseconds -ge 250) {
        $lastProbeUiTick = $now
        Update-ProgressState -StateTable $script:StateTableRef
        Show-ParallelProgress -StateTable $script:StateTableRef -TotalCount $script:TotalWorkCount
      }
    }

    if ($script:StopRequested -or $script:CancellationToken.IsCancellationRequested) {
      try { $probeProcess.Kill() }
      catch {
        Write-VerboseParallelLog -Message "ffprobe cancel cleanup could not kill process for '$InputPath': $($_.Exception.Message)"
      }

      try { $probeProcess.WaitForExit(100) }  # timeout guard
      catch {
        Write-VerboseParallelLog -Message "Exception while waiting for ffprobe process to exit: $($_.Exception.Message)"
      }
      return $null
    }

    if (((Get-Date) - $probeStartedAt).TotalSeconds -gt $script:FFprobeTimeoutSeconds) {
      try { $probeProcess.Kill() }
      catch {
        Write-VerboseParallelLog -Message "ffprobe timeout cleanup could not kill process for '$InputPath': $($_.Exception.Message)"
      }

      try { $probeProcess.WaitForExit(100) }  # timeout guard
      catch {
        Write-VerboseParallelLog -Message "Exception while waiting for ffprobe process to exit: $($_.Exception.Message)"
      }

      Write-VerboseParallelLog -Message "ffprobe timed out after $($script:FFprobeTimeoutSeconds)s for '$InputPath'"
      return $null
    }

    Start-Sleep -Milliseconds 100
  }

  if ($probeProcess.ExitCode -ne 0) {
    $stderr = ""
    try { $stderr = $stderrTask.GetAwaiter().GetResult() }
    catch { $stderr = "" }
    if (-not [string]::IsNullOrWhiteSpace($stderr)) {
      $firstErr = ($stderr -split "`r?`n" | Select-Object -First 1)
      Write-VerboseParallelLog -Message "ffprobe failed for '$InputPath': $firstErr"
    }
    return $null
  }

  $jsonText = ""
  try { $jsonText = $stdoutTask.GetAwaiter().GetResult().Trim() }
  catch { $jsonText = "" }
  if ([string]::IsNullOrWhiteSpace($jsonText)) { return $null }

  try { return ($jsonText | ConvertFrom-Json) }
  catch {
    Write-VerboseParallelLog -Message "ffprobe output was not valid JSON for '$InputPath'."
    return $null
  }
}

function Get-ProbeDuration {
  [OutputType([decimal])]
  param([PSCustomObject]$Probe)
  $ttl = [TimeSpan]::Zero
  if ($Probe -and [TimeSpan]::TryParse("$($Probe.format.duration)", [ref]$ttl)) { return [Math]::Max([decimal]1.0, [decimal]$ttl.TotalMilliseconds) }
  return [decimal]1.0
}

function Get-ResTypeFromDimension {
  param(
    [int]$ResW,
    [int]$ResH
  )

  if (($ResW -eq 0) -or ($ResH -eq 0)) { return [ResTypes]::UNKNOWN }
  elseif (($ResW -le 500) -and ($ResH -le 400)) { return [ResTypes]::SD }
  elseif (($ResW -ge 500) -and ($ResH -le 600)) { return [ResTypes]::DVD }
  elseif (($ResW -ge 900) -and ($ResH -lt 780)) { return [ResTypes]::HD }
  elseif (($ResW -ge 1400) -and ($ResH -lt 1400)) { return [ResTypes]::FHD }
  elseif (($ResW -ge 2000) -and ($ResH -lt 1700)) { return [ResTypes]::QHD }
  elseif (($ResW -ge 3000) -and ($ResH -le 2160)) { return [ResTypes]::UHD_4K }
  elseif ($ResH -gt 2160) { return [ResTypes]::FUHD_8K }

  return [ResTypes]::UNKNOWN
}

function Get-InputFileStatus {
  param(
    [Parameter(Mandatory = $true)][string]$FilePath,
    [Parameter(Mandatory = $false)][switch]$SkipLock
  )

  if ($SkipLock) { return "[S]" }

  try {
    if (-not (Test-Path -LiteralPath $FilePath)) { return "[NF]" }

    try {
      $fs = [System.IO.FileStream]::new(
        $FilePath,
        [System.IO.FileMode]::Open,
        [System.IO.FileAccess]::Read,
        [System.IO.FileShare]::None
      )
      $fs.Dispose()
      return "[W]"
    }
    catch { return "[L]" }
  }
  catch { return "[E]" }
}

function Get-ScaleArgument {
  param(
    [string]$ResizeValue,
    [switch]$RetainAspectValue
  )

  if ([string]::IsNullOrWhiteSpace($ResizeValue)) { return "" }

  $parts = @($ResizeValue.Split(':') | ForEach-Object { $_.Trim() })
  if ($parts.Count -ne 2) { return "" }

  $w = 0
  $h = 0
  if ((-not [int]::TryParse($parts[0], [ref]$w)) -or (-not [int]::TryParse($parts[1], [ref]$h))) { 
    return "" 
  }

  $scaleFilter = 'scale_cuda'
  $scaleSuffix = ':interp_algo=lanczos'
  if ($script:HWProfile) {
    $scaleFilter = $script:HWProfile.ScaleFilter
    $scaleSuffix = $script:HWProfile.ScaleSuffix
  }

  if ($RetainAspectValue) { return "-vf ${scaleFilter}=${w}:-1${scaleSuffix}" }

  return "-vf ${scaleFilter}=${w}:${h}${scaleSuffix}"
}

function Get-TargetResolution {
  param([string]$ResizeValue)

  if ([string]::IsNullOrWhiteSpace($ResizeValue)) { return $null }

  $parts = @($ResizeValue.Split(':') | ForEach-Object { $_.Trim() })
  if ($parts.Count -ne 2) { return $null }

  $w = 0
  $h = 0
  if ((-not [int]::TryParse($parts[0], [ref]$w)) -or (-not [int]::TryParse($parts[1], [ref]$h))) { 
    return $null 
  }

  return [PSCustomObject]@{
    Width  = $w
    Height = $h
  }
}

function Get-ScaleArgumentFromProbe {
  param(
    [string]$ResizeValue,
    [switch]$RetainAspectValue,
    [switch]$ForceResizeValue,
    [switch]$ForceConvertValue,
    [switch]$CanScaleUpValue,
    [switch]$CanScaleDownValue,
    [object]$PrimaryVideoStream
  )

  $targetRes = Get-TargetResolution -ResizeValue $ResizeValue
  if ($null -eq $targetRes) { return "" }

  $srcW = 0
  $srcH = 0
  if ($PrimaryVideoStream) {
    [void][int]::TryParse("$($PrimaryVideoStream.width)", [ref]$srcW)
    [void][int]::TryParse("$($PrimaryVideoStream.height)", [ref]$srcH)
  }

  if ((-not $ForceResizeValue) -and ($srcW -gt 0) -and ($srcH -gt 0)) {
    if (($srcW -eq $targetRes.Width) -and ($srcH -eq $targetRes.Height) -and (-not $ForceConvertValue)) { return "" }

    $srcPixels = $srcW * $srcH
    $dstPixels = $targetRes.Width * $targetRes.Height

    if (($srcPixels -ge $dstPixels) -and (-not $CanScaleDownValue) -and (-not $ForceConvertValue)) { return "" }

    if (($srcPixels -le $dstPixels) -and (-not $CanScaleUpValue) -and (-not $ForceConvertValue)) { return "" }
  }

  $scaleFilter = 'scale_cuda'
  $scaleSuffix = ':interp_algo=lanczos'
  if ($script:HWProfile) {
    $scaleFilter = $script:HWProfile.ScaleFilter
    $scaleSuffix = $script:HWProfile.ScaleSuffix
  }

  if ($RetainAspectValue) { return "-vf ${scaleFilter}=$($targetRes.Width):-1${scaleSuffix}" }

  return "-vf ${scaleFilter}=$($targetRes.Width):$($targetRes.Height)${scaleSuffix}"
}

function Get-AudioMapArg {
  param([string[]]$AudioLangValue, [object[]]$AudioStreams)

  $langValues = @($AudioLangValue | ForEach-Object {
      if ($null -eq $_) { return }
      foreach ($part in ("$_" -split ',')) {
        $token = $part.Trim().ToLowerInvariant()
        if (-not [string]::IsNullOrWhiteSpace($token)) { $token }
      }
    } | Select-Object -Unique)
  if ($langValues.Count -eq 0) { $langValues = @('eng') }

  $audioCount = @($AudioStreams).Count
  if ($langValues -contains "nomap" -or $langValues -contains "none") { return "" }
  if ($langValues -contains "all") {
    if ($audioCount -eq 0) { return "-map 0:a?" }
    return "-map 0:a"
  }

  $mapArgs = New-Object System.Collections.Generic.List[string]
  foreach ($lang in $langValues) {
    $matching = @($AudioStreams | Where-Object { $_.tags -and $_.tags.language -ieq $lang })
    if ($matching.Count -gt 0) {
      [void]$mapArgs.Add("-map 0:a:m:language:$lang")
    }
  }
  if ($mapArgs.Count -gt 0) { return ($mapArgs -join ' ') }

  if ($audioCount -eq 0) { return "-map 0:a?" }
  return "-map 0:a"
}

function Test-UndesiredDefaultStream {
  param([object]$Stream)

  if ($null -eq $Stream) { return $true }

  $disp = $Stream.disposition
  if ($disp) {
    if (([int]($disp.comment -as [int])) -eq 1) { return $true }
    if (([int]($disp.hearing_impaired -as [int])) -eq 1) { return $true }
    if (([int]($disp.visual_impaired -as [int])) -eq 1) { return $true }
  }

  $tags = $Stream.tags
  if ($tags) {
    $tagText = @(
      [string]$tags.title,
      [string]$tags.comment,
      [string]$tags.handler_name
    ) -join ' '

    if ($tagText -match '(?i)commentary|director''?s\s+commentary|audio\s+description|descriptive\s+audio|description\s+track|hearing\s*impaired|\bsd?h\b') {
      return $true
    }
  }

  return $false
}

function Get-LanguageMapPlan {
  param(
    [string[]]$LangValues,
    [object[]]$Streams,
    [ValidateSet('a', 's')][string]$TypeToken
  )

  $normalized = @($LangValues | ForEach-Object {
      if ($null -eq $_) { return }
      foreach ($part in ("$_" -split ',')) {
        $token = $part.Trim().ToLowerInvariant()
        if (-not [string]::IsNullOrWhiteSpace($token)) { $token }
      }
    } | Select-Object -Unique)
  if ($normalized.Count -eq 0) { $normalized = @('eng') }

  $streamCount = @($Streams).Count
  if ($normalized -contains 'nomap' -or $normalized -contains 'none') {
    return [PSCustomObject]@{ MapArg = ''; MappedStreams = @(); Normalized = $normalized }
  }

  if ($normalized -contains 'all') {
    if ($streamCount -eq 0) {
      return [PSCustomObject]@{ MapArg = "-map 0:${TypeToken}?"; MappedStreams = @(); Normalized = $normalized }
    }
    return [PSCustomObject]@{ MapArg = "-map 0:${TypeToken}"; MappedStreams = @($Streams); Normalized = $normalized }
  }

  $mapArgs = New-Object System.Collections.Generic.List[string]
  $mappedStreams = New-Object System.Collections.Generic.List[object]
  foreach ($lang in $normalized) {
    $matching = @($Streams | Where-Object { $_.tags -and $_.tags.language -ieq $lang })
    if ($matching.Count -gt 0) {
      [void]$mapArgs.Add("-map 0:${TypeToken}:m:language:$lang")
      foreach ($m in $matching) { [void]$mappedStreams.Add($m) }
    }
  }

  if ($mapArgs.Count -gt 0) {
    return [PSCustomObject]@{ MapArg = ($mapArgs -join ' '); MappedStreams = $mappedStreams.ToArray(); Normalized = $normalized }
  }

  if ($streamCount -eq 0) {
    return [PSCustomObject]@{ MapArg = "-map 0:${TypeToken}?"; MappedStreams = @(); Normalized = $normalized }
  }

  return [PSCustomObject]@{ MapArg = "-map 0:${TypeToken}"; MappedStreams = @($Streams); Normalized = $normalized }
}

function Get-PreferredMappedDefaultIndex {
  param([object[]]$MappedStreams)

  if (-not $MappedStreams -or $MappedStreams.Count -eq 0) { return -1 }

  for ($i = 0; $i -lt $MappedStreams.Count; $i++) {
    if (-not (Test-UndesiredDefaultStream -Stream $MappedStreams[$i])) {
      return $i
    }
  }

  return 0
}

function Get-SubMapArg {
  param([string[]]$SubLangValue, [object[]]$SubStreams)

  $langValues = @($SubLangValue | ForEach-Object {
      if ($null -eq $_) { return }
      foreach ($part in ("$_" -split ',')) {
        $token = $part.Trim().ToLowerInvariant()
        if (-not [string]::IsNullOrWhiteSpace($token)) { $token }
      }
    } | Select-Object -Unique)
  if ($langValues.Count -eq 0) { $langValues = @('eng') }

  $subCount = @($SubStreams).Count
  if ($langValues -contains "nomap" -or $langValues -contains "none") { return "" }
  if ($langValues -contains "all") {
    if ($subCount -eq 0) { return "-map 0:s?" }
    return "-map 0:s"
  }

  $mapArgs = New-Object System.Collections.Generic.List[string]
  foreach ($lang in $langValues) {
    $matching = @($SubStreams | Where-Object { $_.tags -and $_.tags.language -ieq $lang })
    if ($matching.Count -gt 0) {
      [void]$mapArgs.Add("-map 0:s:m:language:$lang")
    }
  }
  if ($mapArgs.Count -gt 0) { return ($mapArgs -join ' ') }

  if ($subCount -eq 0) { return "-map 0:s?" }
  return "-map 0:s"
}

function Get-EncodeAnalysis {
  param([object]$Probe)

  $videoStreams = @($Probe.streams | Where-Object { $_.codec_type -ieq 'video' })
  $audioStreams = @($Probe.streams | Where-Object { $_.codec_type -ieq 'audio' })
  $subStreams = @($Probe.streams | Where-Object { $_.codec_type -ieq 'subtitle' })
  $primaryVideo = $videoStreams | Select-Object -First 1

  $audioPlan = Get-LanguageMapPlan -LangValues $AudioLang -Streams $audioStreams -TypeToken 'a'
  $subPlan = Get-LanguageMapPlan -LangValues $SubLang -Streams $subStreams -TypeToken 's'
  $audioMap = $audioPlan.MapArg
  $subMap = $subPlan.MapArg
  $audioDisposition = ""
  $subDisposition = ""
  $preferredAudioIndex = Get-PreferredMappedDefaultIndex -MappedStreams $audioPlan.MappedStreams
  if ($preferredAudioIndex -ge 0 -and -not [string]::IsNullOrWhiteSpace($audioMap)) {
    # Ensure deterministic default: clear output audio dispositions, then set preferred mapped audio as default.
    $audioDisposition = "-disposition:a 0 -disposition:a:$preferredAudioIndex default"
  }
  $preferredSubIndex = Get-PreferredMappedDefaultIndex -MappedStreams $subPlan.MappedStreams
  if ($preferredSubIndex -ge 0 -and -not [string]::IsNullOrWhiteSpace($subMap)) {
    # Ensure deterministic default: clear output subtitle dispositions, then set preferred mapped subtitle as default.
    $subDisposition = "-disposition:s 0 -disposition:s:$preferredSubIndex default"
  }
  $scaleArg = Get-ScaleArgumentFromProbe -ResizeValue $ResizeResolution -RetainAspectValue:$RetainAspect -ForceResizeValue:$ForceResize -ForceConvertValue:$ForceConvert -CanScaleUpValue:$CanScaleUp -CanScaleDownValue:$CanScaleDown -PrimaryVideoStream $primaryVideo

  return [PSCustomObject]@{
    AudioMap     = $audioMap
    SubMap       = $subMap
    AudioDisp    = $audioDisposition
    SubDisp      = $subDisposition
    ScaleArg     = $scaleArg
    PrimaryVideo = $primaryVideo
  }
}

function Get-HardwareEncoderProfile {
  [OutputType([PSCustomObject])]
  param(
    [Parameter(Mandatory = $false)][ValidateSet('auto', 'nvenc', 'amf', 'qsv', 'software')][string]$PreferredEncoder = 'auto'
  )

  function New-HwAccelOption {
    param(
      [Parameter(Mandatory = $true)][string]$Name,
      [Parameter(Mandatory = $false)][string]$HwaccelArgs = '',
      [Parameter(Mandatory = $true)][string]$ScaleFilter,
      [Parameter(Mandatory = $false)][string]$ScaleSuffix = ''
    )

    return [PSCustomObject]@{
      Name        = $Name
      Args        = $HwaccelArgs
      ScaleFilter = $ScaleFilter
      ScaleSuffix = $ScaleSuffix
    }
  }

  function Get-AvailableHwAccelSet {
    param([string]$HwaccelsText)

    $set = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    if ([string]::IsNullOrWhiteSpace($HwaccelsText)) { return $set }

    foreach ($line in ($HwaccelsText -split "`r?`n")) {
      $name = $line.Trim()
      if ([string]::IsNullOrWhiteSpace($name)) { continue }
      if ($name -match '^(Hardware acceleration methods:|ffmpeg version|configuration:|libav)') { continue }
      if ($name -notmatch '^[a-z0-9_]+$') { continue }
      [void]$set.Add($name)
    }

    return $set
  }

  function Select-HwAccelOption {
    param(
      [Parameter(Mandatory = $true)][object[]]$Options,
      [Parameter(Mandatory = $true)][System.Collections.Generic.HashSet[string]]$AvailableHwaccels
    )

    foreach ($option in $Options) {
      if ($option.Name -in @('none', 'auto')) { return $option }
      if ($AvailableHwaccels.Contains($option.Name)) { return $option }
    }

    return (New-HwAccelOption -Name 'none' -Args '' -ScaleFilter 'scale' -ScaleSuffix ':flags=lanczos')
  }

  function Build-Profile {
    param(
      [Parameter(Mandatory = $true)][string]$ProfileName,
      [Parameter(Mandatory = $true)][string]$Encoder,
      [Parameter(Mandatory = $true)][scriptblock]$CodecArgs,
      [Parameter(Mandatory = $true)][object[]]$HwAccelOptions,
      [Parameter(Mandatory = $true)][System.Collections.Generic.HashSet[string]]$AvailableHwaccels
    )

    $selectedAccel = Select-HwAccelOption -Options $HwAccelOptions -AvailableHwaccels $AvailableHwaccels
    $selectedIndex = [Array]::IndexOf($HwAccelOptions, $selectedAccel)

    $fallbackChain = @()
    for ($i = $selectedIndex; $i -lt ($HwAccelOptions.Count - 1); $i++) {
      $fromOpt = $HwAccelOptions[$i]
      $toOpt = $HwAccelOptions[$i + 1]
      $fallbackChain += {
        param([string]$HwaccelArgs)
        $next = $HwaccelArgs
        if ($fromOpt.Args -ne $toOpt.Args) {
          $next = $next.Replace($fromOpt.Args, $toOpt.Args)
        }
        if ($fromOpt.ScaleFilter -ne $toOpt.ScaleFilter) {
          $next = $next.Replace("$($fromOpt.ScaleFilter)=", "$($toOpt.ScaleFilter)=")
        }
        if ($fromOpt.ScaleSuffix -ne $toOpt.ScaleSuffix) {
          $next = $next.Replace($fromOpt.ScaleSuffix, $toOpt.ScaleSuffix)
        }
        return $next
      }.GetNewClosure()
    }

    return [PSCustomObject]@{
      Name          = $ProfileName
      Encoder       = $Encoder
      HwaccelName   = $selectedAccel.Name
      HwaccelArgs   = $selectedAccel.Args
      ScaleFilter   = $selectedAccel.ScaleFilter
      ScaleSuffix   = $selectedAccel.ScaleSuffix
      CodecArgs     = $CodecArgs
      FallbackChain = $fallbackChain
    }
  }

  $encodersText = ''
  $hwaccelsText = ''
  try {
    $encodersText = (& $script:ffmpeg_exe -hide_banner -encoders 2>&1 | Out-String)
    $hwaccelsText = (& $script:ffmpeg_exe -hide_banner -hwaccels 2>&1 | Out-String)
  }
  catch {
    Write-ParallelLog -Message "Unable to query ffmpeg capabilities: $($_.Exception.Message). Falling back to software." -Level Warning -Target Both
    return [PSCustomObject]@{
      Name          = 'software'
      Encoder       = 'libx265'
      HwaccelName   = 'none'
      HwaccelArgs   = ''
      ScaleFilter   = 'scale'
      ScaleSuffix   = ':flags=lanczos'
      CodecArgs     = {
        param([int]$Cqp, [string]$BitrateArg)
        return "-crf $Cqp -preset slow "
      }
      FallbackChain = @()
    }
  }

  $availableHwaccels = Get-AvailableHwAccelSet -HwaccelsText $hwaccelsText
  $hasNvenc = $encodersText -match '(?im)\bhevc_nvenc\b'
  $hasAmf = $encodersText -match '(?im)\bhevc_amf\b'
  $hasQsv = $encodersText -match '(?im)\bhevc_qsv\b'

  $nvencProfile = [PSCustomObject]@{
    Name = 'unused'
  }

  $amfProfile = [PSCustomObject]@{
    Name = 'unused'
  }

  $qsvProfile = [PSCustomObject]@{
    Name = 'unused'
  }

  $softwareProfile = [PSCustomObject]@{
    Name          = 'software'
    Encoder       = 'libx265'
    HwaccelName   = 'none'
    HwaccelArgs   = ''
    ScaleFilter   = 'scale'
    ScaleSuffix   = ':flags=lanczos'
    CodecArgs     = {
      param([int]$Cqp, [string]$BitrateArg)
      return "-crf $Cqp -preset slow "
    }
    FallbackChain = @()
  }

  $nvencOptions = @(
    (New-HwAccelOption -Name 'cuda' -HwaccelArgs '-hwaccel cuda -hwaccel_output_format cuda' -ScaleFilter 'scale_cuda' -ScaleSuffix ':interp_algo=lanczos')
    (New-HwAccelOption -Name 'cuda' -HwaccelArgs '-hwaccel cuda' -ScaleFilter 'scale_cuda' -ScaleSuffix ':interp_algo=lanczos')
    (New-HwAccelOption -Name 'auto' -HwaccelArgs '-hwaccel auto' -ScaleFilter 'scale' -ScaleSuffix ':flags=lanczos')
    (New-HwAccelOption -Name 'none' -HwaccelArgs '' -ScaleFilter 'scale' -ScaleSuffix ':flags=lanczos')
  )

  $amfOptions = @(
    (New-HwAccelOption -Name 'd3d11va' -HwaccelArgs '-hwaccel d3d11va -hwaccel_output_format d3d11' -ScaleFilter 'scale' -ScaleSuffix ':flags=lanczos')
    (New-HwAccelOption -Name 'dxva2' -HwaccelArgs '-hwaccel dxva2' -ScaleFilter 'scale' -ScaleSuffix ':flags=lanczos')
    (New-HwAccelOption -Name 'vaapi' -HwaccelArgs '-hwaccel vaapi -hwaccel_output_format vaapi' -ScaleFilter 'scale_vaapi' -ScaleSuffix ':format=nv12')
    (New-HwAccelOption -Name 'vulkan' -HwaccelArgs '-hwaccel vulkan -hwaccel_output_format vulkan' -ScaleFilter 'scale' -ScaleSuffix ':flags=lanczos')
    (New-HwAccelOption -Name 'auto' -HwaccelArgs '-hwaccel auto' -ScaleFilter 'scale' -ScaleSuffix ':flags=lanczos')
    (New-HwAccelOption -Name 'none' -HwaccelArgs '' -ScaleFilter 'scale' -ScaleSuffix ':flags=lanczos')
  )

  $qsvOptions = @(
    (New-HwAccelOption -Name 'qsv' -HwaccelArgs '-hwaccel qsv -hwaccel_output_format qsv' -ScaleFilter 'scale_qsv' -ScaleSuffix '')
    (New-HwAccelOption -Name 'qsv' -HwaccelArgs '-hwaccel qsv' -ScaleFilter 'scale' -ScaleSuffix ':flags=lanczos')
    (New-HwAccelOption -Name 'vaapi' -HwaccelArgs '-hwaccel vaapi -hwaccel_output_format vaapi' -ScaleFilter 'scale_vaapi' -ScaleSuffix ':format=nv12')
    (New-HwAccelOption -Name 'auto' -HwaccelArgs '-hwaccel auto' -ScaleFilter 'scale' -ScaleSuffix ':flags=lanczos')
    (New-HwAccelOption -Name 'none' -HwaccelArgs '' -ScaleFilter 'scale' -ScaleSuffix ':flags=lanczos')
  )

  $nvencProfile = Build-Profile -ProfileName 'nvenc' -Encoder 'hevc_nvenc' -CodecArgs {
    param([int]$Cqp, [string]$BitrateArg)
    return "$BitrateArg-bufsize:v 5MB -preset:v p4 -tune:v hq -tier:v main -rc:v constqp " +
    "-init_qpI:v $Cqp -init_qpP:v $($Cqp + 1) -init_qpB:v $($Cqp + 2) " +
    "-rc-lookahead:v 32 -spatial-aq:v 1 -aq-strength:v 8 -temporal-aq:v 1 -b_ref_mode:v 1 "
  } -HwAccelOptions $nvencOptions -AvailableHwaccels $availableHwaccels

  $amfProfile = Build-Profile -ProfileName 'amf' -Encoder 'hevc_amf' -CodecArgs {
    param([int]$Cqp, [string]$BitrateArg)
    return "$BitrateArg-quality balanced -rc cqp -qp_i $Cqp -qp_p $($Cqp + 1) -qp_b $($Cqp + 2) "
  } -HwAccelOptions $amfOptions -AvailableHwaccels $availableHwaccels

  $qsvProfile = Build-Profile -ProfileName 'qsv' -Encoder 'hevc_qsv' -CodecArgs {
    param([int]$Cqp, [string]$BitrateArg)
    return "$BitrateArg-look_ahead 1 -global_quality $Cqp "
  } -HwAccelOptions $qsvOptions -AvailableHwaccels $availableHwaccels

  $profilesByName = @{
    nvenc    = $nvencProfile
    amf      = $amfProfile
    qsv      = $qsvProfile
    software = $softwareProfile
  }

  if ($PreferredEncoder -ne 'auto') {
    $selectedProfile = $profilesByName[$PreferredEncoder]
    Write-ParallelLog -Message "Encoder profile forced to '$($selectedProfile.Name)' by parameter -Encoder (hwaccel='$($selectedProfile.HwaccelName)')." -Target Both
    return $selectedProfile
  }

  $selectedProfile = $softwareProfile
  if ($hasNvenc) { $selectedProfile = $nvencProfile }
  elseif ($hasAmf) { $selectedProfile = $amfProfile }
  elseif ($hasQsv) { $selectedProfile = $qsvProfile }

  Write-ParallelLog -Message "Encoder auto-detection: Compiled encoders available (hevc_nvenc=$hasNvenc, hevc_amf=$hasAmf, hevc_qsv=$hasQsv)." -Target Both
  Write-ParallelLog -Message "Encoder auto-detection selected: '$($selectedProfile.Name)' (hwaccel='$($selectedProfile.HwaccelName)')." -Target Both
  return $selectedProfile
}

function Invoke-EncodeTestWithFallback {
  param(
    [Parameter(Mandatory = $true)][string]$FFmpegArgs,
    [Parameter(Mandatory = $true)][string]$OutputFile,
    [Parameter(Mandatory = $true)][int]$JobId
  )

  $hwProfile = $script:HWProfile
  if (-not $hwProfile) { throw "Hardware profile is not initialized." }

  $nullSinkArgs = "-ss 0 -to 10 -f null $($script:NullDevice)"
  $testArgs = $FFmpegArgs.Replace('"' + $OutputFile + '"', $nullSinkArgs)
  $fallbackSteps = @($hwProfile.FallbackChain)
  $maxAttempts = [Math]::Max(1, $fallbackSteps.Count + 1)
  $attempt = 1

  while ($attempt -le $maxAttempts) {
    Write-VerboseParallelLog -Message "Running ffmpeg test attempt $attempt/$maxAttempts ($($hwProfile.Name)) with args: $testArgs" -JobId $JobId

    $p = New-Object System.Diagnostics.Process
    $p.StartInfo.FileName = $script:ffmpeg_exe
    $p.StartInfo.Arguments = $testArgs
    $p.StartInfo.UseShellExecute = $false
    $p.StartInfo.RedirectStandardOutput = $false
    $p.StartInfo.RedirectStandardError = $true
    $p.StartInfo.CreateNoWindow = $true

    $p.Start() | Out-Null
    $stderrTask = $p.StandardError.ReadToEndAsync()
    $p.WaitForExit()
    $exitCode = $p.ExitCode

    if ($exitCode -eq 0) {
      $effectiveArgs = $testArgs.Replace($nullSinkArgs, '"' + $OutputFile + '"')
      Write-VerboseParallelLog -Message "ffmpeg test succeeded; effective encode args selected." -JobId $JobId
      Write-VerboseParallelLog -Message "Effective ffmpeg args: $effectiveArgs" -JobId $JobId
      return $effectiveArgs
    }

    $err = ""
    try { $err = $stderrTask.GetAwaiter().GetResult().Trim() }
    catch { $err = "" }
    $errSummary = "Unknown ffmpeg test error"
    if (-not [string]::IsNullOrWhiteSpace($err)) { $errSummary = ($err -split "`r?`n")[0].Trim() }

    Write-ParallelLog -Message "Test command failed (attempt $attempt/$maxAttempts): $errSummary" -Level Warning -Target Both -JobId $JobId

    $fallbackIndex = $attempt - 1
    if ($fallbackIndex -ge $fallbackSteps.Count) { break }

    try {
      $testArgs = & $fallbackSteps[$fallbackIndex] $testArgs
      Write-ParallelLog -Message "Applying fallback step $($fallbackIndex + 1)/$($fallbackSteps.Count) for encoder '$($hwProfile.Name)'." -Level Warning -Target Both -JobId $JobId
    }
    catch {
      Write-ParallelLog -Message "Fallback step $($fallbackIndex + 1) failed: $($_.Exception.Message)" -Level Warning -Target Both -JobId $JobId
    }

    Start-Sleep -Seconds ([Math]::Pow(2, $attempt))
    $attempt++
  }

  throw "ffmpeg test command failed after $maxAttempts attempts for encoder '$($hwProfile.Name)'."
}

function ConvertTo-BaseNameCodecTag {
  [OutputType([string])]
  param([string]$BaseName)

  $replaceMap = @{
    "h265" = @("h.264", "x.264", "h264", "x264", "x265", "xvid", "hevc", "avc", "av1", "mpeg2")
    ""     = @("raw-hd")
  }

  foreach ($newValue in $replaceMap.Keys) {
    foreach ($oldValue in $replaceMap[$newValue]) {
      $BaseName = [System.Text.RegularExpressions.Regex]::Replace($BaseName, [Regex]::Escape($oldValue), $newValue, "IgnoreCase")
    }
  }

  # Collapse consecutive whitespace to a single space.
  $BaseName = [System.Text.RegularExpressions.Regex]::Replace($BaseName, "\s{2,}", " ").Trim()
  return $BaseName
}

function Test-AnalyzeCancellationRequested {
  [OutputType([bool])]
  param()

  return ($script:StopRequested -or $script:CancellationToken.IsCancellationRequested)
}

function Register-AnalyzeTempArtifact {
  param([Parameter(Mandatory = $true)][string]$Path)

  if ([string]::IsNullOrWhiteSpace($Path)) { return }
  if (-not $script:AnalyzeTempArtifacts.Contains($Path)) {
    [void]$script:AnalyzeTempArtifacts.Add($Path)
  }
}

function Unregister-AnalyzeTempArtifact {
  param([Parameter(Mandatory = $true)][string]$Path)

  if ([string]::IsNullOrWhiteSpace($Path)) { return }
  [void]$script:AnalyzeTempArtifacts.Remove($Path)
}

function Remove-AnalyzeTempArtifacts {
  param([Parameter(Mandatory = $false)][string]$OutputDir)

  if (-not [string]::IsNullOrWhiteSpace($OutputDir) -and (Test-Path -LiteralPath $OutputDir -PathType Container -ErrorAction SilentlyContinue)) {
    foreach ($stale in @(Get-ChildItem -LiteralPath $OutputDir -File -Filter 'metadata*.tmp*' -ErrorAction SilentlyContinue)) {
      try { Remove-Item -LiteralPath $stale.FullName -Force -ErrorAction Stop }
      catch { Write-VerboseParallelLog -Message "Unable to remove stale analyze artifact '$($stale.FullName)': $($_.Exception.Message)" }
    }

    foreach ($staleInProgress in @(Get-ChildItem -LiteralPath $OutputDir -File -Filter 'metadata*.inprogress*' -ErrorAction SilentlyContinue)) {
      try { Remove-Item -LiteralPath $staleInProgress.FullName -Force -ErrorAction Stop }
      catch { Write-VerboseParallelLog -Message "Unable to remove stale analyze in-progress artifact '$($staleInProgress.FullName)': $($_.Exception.Message)" }
    }
  }

  foreach ($artifactPath in @($script:AnalyzeTempArtifacts)) {
    if ([string]::IsNullOrWhiteSpace($artifactPath)) { continue }
    try {
      if (Test-Path -LiteralPath $artifactPath -PathType Leaf -ErrorAction SilentlyContinue) {
        Remove-Item -LiteralPath $artifactPath -Force -ErrorAction Stop
      }
    }
    catch {
      Write-VerboseParallelLog -Message "Unable to remove analyze temp artifact '$artifactPath': $($_.Exception.Message)"
    }
    finally {
      [void]$script:AnalyzeTempArtifacts.Remove($artifactPath)
    }
  }
}

function Get-FileMetadata {
  [OutputType([hashtable])]
  param(
    [Parameter(Mandatory = $true)][string]$FilePath,
    [Parameter(Mandatory = $false)][string]$HashAlgorithm = 'size-mtime'
  )

  $metadata = @{
    path      = $FilePath
    size      = $null
    mtime     = $null
    ctime     = $null
    hash      = $null
    duration  = $null
    video     = $null
    audio     = @()
    subtitles = @()
    tags      = @()
  }

  try {
    $fileInfo = Get-Item -LiteralPath $FilePath -ErrorAction Stop
    $metadata.size = $fileInfo.Length
    $metadata.mtime = $fileInfo.LastWriteTimeUtc.ToString('o')
    $metadata.ctime = $fileInfo.CreationTimeUtc.ToString('o')
  }
  catch {
    Write-ParallelLog -Message "Failed to get file system metadata for '$FilePath': $($_.Exception.Message)" -Level Error
    return $null
  }

  try {
    if ($HashAlgorithm -eq 'size-mtime') {
      $metadata.hash = "size-mtime:$($metadata.size):$($fileInfo.LastWriteTimeUtc.Ticks)"
    }
    else {
      # CRC32 via shared helper for consistency with other script hashing use.
      $metadata.hash = 'crc32:{0}' -f (Get-Crc32HexFromFile -FilePath $FilePath)
    }
  }
  catch {
    Write-ParallelLog -Message "Failed to compute hash for '$FilePath': $($_.Exception.Message)" -Level Error
    return $null
  }

  # Only run ffprobe on file types it can meaningfully parse (video + audio containers).
  # Images, ebooks, subtitles and other companion files get filesystem-only metadata.
  $ffprobeCapableExts = @(
    '.avi', '.divx', '.mkv', '.mp4', '.m4v', '.m2ts', '.mts', '.mov', '.mpg', '.mpeg',
    '.ts', '.wmv', '.flv', '.webm', '.3gp', '.3g2', '.vob', '.f4v', '.ogv', '.rm', '.rmvb', '.asf',
    '.mp3', '.flac', '.aac', '.m4a', '.ogg', '.opus', '.wav', '.wma', '.aiff', '.aif',
    '.alac', '.ape', '.wv', '.mka', '.dts', '.ac3', '.eac3', '.dsf', '.dff'
  )
  $fileExt = [System.IO.Path]::GetExtension($FilePath).ToLowerInvariant()

  if ($ffprobeCapableExts -contains $fileExt) {
    try {
      $probe = Get-FFprobeJson -InputPath $FilePath -AnalyzeMode
      if ($probe) {
        $metadata.duration = [math]::Round(($probe.format.duration -as [double]) * 1000)  # in milliseconds
        if ($probe.format.tags) {
          $metadata.tags = $probe.format.tags.PSObject.Properties | ForEach-Object { "$($_.Name):$($_.Value)" }
        }
        if ($probe.streams) {
          $videoStream = $probe.streams | Where-Object { $_.codec_type -eq 'video' } | Select-Object -First 1
          if ($videoStream) {
            $metadata.video = @{
              codec   = $videoStream.codec_name
              bitrate = [int]$videoStream.bit_rate
              width   = [int]$videoStream.width
              height  = [int]$videoStream.height
              hdr     = ($videoStream.color_space -eq 'bt2020nc' -or $videoStream.color_transfer -eq 'smpte2084')
            }
          }
          $audioStreams = $probe.streams | Where-Object { $_.codec_type -eq 'audio' }
          $metadata.audio = $audioStreams | ForEach-Object {
            @{
              lang     = $_.tags.language
              codec    = $_.codec_name
              channels = [int]$_.channels
            }
          }
          $subtitleStreams = $probe.streams | Where-Object { $_.codec_type -eq 'subtitle' }
          $metadata.subtitles = $subtitleStreams | ForEach-Object { $_.tags.language } | Where-Object { $_ } | Sort-Object -Unique
        }
      }
    }
    catch {
      Write-ParallelLog -Message "Failed to probe '$FilePath': $($_.Exception.Message)" -Level Error
      return $null
    }
  }

  return $metadata
}

function Export-MetadataToNDJSON {
  param(
    [Parameter(Mandatory = $true)][array]$MetadataList,
    [Parameter(Mandatory = $true)][string]$OutputDir,
    [Parameter(Mandatory = $false)][switch]$GenerateHtml
  )

  $ndjsonPath = Join-Path $OutputDir "metadata.ndjson"
  $htmlPath = Join-Path $OutputDir "metadata_report.html"

  function Get-ImpliedLibraryName {
    param(
      [Parameter(Mandatory = $false)][string]$PathValue
    )

    if ([string]::IsNullOrWhiteSpace($PathValue)) { return "(root)" }

    $parts = $PathValue -split '[\\/]+' | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    if ($parts.Count -eq 0) { return "(root)" }

    $mediaIdx = -1
    for ($i = 0; $i -lt $parts.Count; $i++) {
      if ($parts[$i].ToLowerInvariant() -eq 'media') { $mediaIdx = $i; break }
    }

    if ($mediaIdx -ge 0 -and ($mediaIdx + 1) -lt $parts.Count) {
      return $parts[$mediaIdx + 1]
    }

    if ($parts.Count -gt 1 -and $parts[0] -match '^[A-Za-z]:$') {
      return $parts[1]
    }

    return $parts[0]
  }

  # Append new metadata to NDJSON
  $tempNdjson = "$ndjsonPath.$([guid]::NewGuid().ToString('N')).tmp"
  Register-AnalyzeTempArtifact -Path $tempNdjson
  $stream = $null
  $writer = $null
  try {
    if (Test-AnalyzeCancellationRequested) {
      Write-ParallelLog -Message "Analyze cancellation requested before NDJSON write; skipping export." -Level Warning -Target Both
      return
    }

    $stream = [System.IO.File]::Open($tempNdjson, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
    $writer = [System.IO.StreamWriter]::new($stream, [System.Text.Encoding]::UTF8)
    foreach ($meta in $MetadataList) {
      if (Test-AnalyzeCancellationRequested) {
        Write-ParallelLog -Message "Analyze cancellation requested during NDJSON write; dropping temp artifact." -Level Warning -Target Both
        return
      }

      if ($meta) {
        $metaForExport = $meta
        $existingLibraryProp = $meta.PSObject.Properties['library']
        if (-not $existingLibraryProp -or [string]::IsNullOrWhiteSpace([string]$existingLibraryProp.Value)) {
          $libraryName = Get-ImpliedLibraryName -PathValue ([string]$meta.path)
          $metaForExport = [PSCustomObject]@{}
          foreach ($prop in $meta.PSObject.Properties) {
            $metaForExport | Add-Member -NotePropertyName $prop.Name -NotePropertyValue $prop.Value
          }
          $metaForExport | Add-Member -NotePropertyName 'library' -NotePropertyValue $libraryName
        }

        $json = $metaForExport | ConvertTo-Json -Compress -Depth 10
        $writer.WriteLine($json)
      }
    }
    $writer.Flush()
    $writer.Dispose()
    $writer = $null
    $stream.Dispose()
    $stream = $null

    if (Test-AnalyzeCancellationRequested) {
      Write-ParallelLog -Message "Analyze cancellation requested before NDJSON finalize; dropping temp artifact." -Level Warning -Target Both
      return
    }

    # Atomic replace
    [System.IO.File]::Move($tempNdjson, $ndjsonPath, $true)
    Unregister-AnalyzeTempArtifact -Path $tempNdjson
  }
  catch {
    Write-ParallelLog -Message "Failed to write NDJSON: $($_.Exception.Message)" -Level Error
    if ($writer) { try { $writer.Dispose() } catch { } }
    if ($stream) { try { $stream.Dispose() } catch { } }
    if (Test-Path -LiteralPath $tempNdjson -PathType Leaf -ErrorAction SilentlyContinue) { Remove-Item -LiteralPath $tempNdjson -Force -ErrorAction SilentlyContinue }
    Unregister-AnalyzeTempArtifact -Path $tempNdjson
    return
  }

  # Generate HTML if requested
  if ($GenerateHtml) {
    try {
      $generatedAt = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
      $templatePath = Join-Path $PSScriptRoot 'metadata_report_template.html'
      if (-not (Test-Path -LiteralPath $templatePath -PathType Leaf -ErrorAction SilentlyContinue)) {
        throw "HTML template not found at: $templatePath"
      }
      $entryCount = $MetadataList.Count.ToString('N0')
      $html = (Get-Content -LiteralPath $templatePath -Raw -Encoding UTF8).Replace(
        '<!-- REPORT_META -->',
        "Generated: $generatedAt &bull; $entryCount entries"
      )
      $html | Out-File -LiteralPath $htmlPath -Encoding UTF8
    }
    catch {
      Write-ParallelLog -Message "Failed to generate HTML: $($_.Exception.Message)" -Level Error
    }
  }
}

function Invoke-CompactMetadata {
  param(
    [Parameter(Mandatory = $true)][string]$OutputDir
  )

  $ndjsonPath = Join-Path $OutputDir "metadata.ndjson"
  if (Test-AnalyzeCancellationRequested) {
    Write-ParallelLog -Message "Analyze cancellation requested before compaction; skipping compaction." -Level Warning -Target Both
    return
  }

  if (-not (Test-Path $ndjsonPath)) {
    Write-ParallelLog -Message "No NDJSON file found for compaction" -Level Warning
    return
  }

  # Read all entries
  $entries = @()
  try {
    $lines = Get-Content $ndjsonPath -Encoding UTF8
    foreach ($line in $lines) {
      if ($line.Trim()) {
        $entries += $line | ConvertFrom-Json
      }
    }
  }
  catch {
    Write-ParallelLog -Message "Failed to read NDJSON for compaction: $($_.Exception.Message)" -Level Error
    return
  }

  # Group by path, keep latest (assuming append order)
  $latestEntries = @{}
  foreach ($entry in $entries) {
    $latestEntries[$entry.path] = $entry
  }

  # Filter out deleted files
  $currentEntries = $latestEntries.Values | Where-Object {
    Test-Path -LiteralPath $_.path -ErrorAction SilentlyContinue
  }

  # Rewrite NDJSON
  $tempNdjson = "$ndjsonPath.$([guid]::NewGuid().ToString('N')).tmp"
  Register-AnalyzeTempArtifact -Path $tempNdjson
  try {
    if (Test-AnalyzeCancellationRequested) {
      Write-ParallelLog -Message "Analyze cancellation requested during compaction setup; dropping temp artifact." -Level Warning -Target Both
      return
    }

    $stream = [System.IO.File]::Open($tempNdjson, [System.IO.FileMode]::Create, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
    $writer = [System.IO.StreamWriter]::new($stream, [System.Text.Encoding]::UTF8)
    foreach ($entry in $currentEntries) {
      if (Test-AnalyzeCancellationRequested) {
        Write-ParallelLog -Message "Analyze cancellation requested during compaction write; dropping temp artifact." -Level Warning -Target Both
        $writer.Dispose()
        $stream.Dispose()
        return
      }

      $json = $entry | ConvertTo-Json -Compress -Depth 10
      $writer.WriteLine($json)
    }
    $writer.Flush()
    $writer.Dispose()
    $stream.Dispose()

    if (Test-AnalyzeCancellationRequested) {
      Write-ParallelLog -Message "Analyze cancellation requested before compacted NDJSON finalize; dropping temp artifact." -Level Warning -Target Both
      return
    }

    # Atomic replace
    [System.IO.File]::Move($tempNdjson, $ndjsonPath, $true)
    Unregister-AnalyzeTempArtifact -Path $tempNdjson
  }
  catch {
    Write-ParallelLog -Message "Failed to compact NDJSON: $($_.Exception.Message)" -Level Error
    if (Test-Path -LiteralPath $tempNdjson -PathType Leaf -ErrorAction SilentlyContinue) { Remove-Item -LiteralPath $tempNdjson -Force -ErrorAction SilentlyContinue }
    Unregister-AnalyzeTempArtifact -Path $tempNdjson
    return
  }

  Write-ParallelLog -Message "Compacted metadata: $($currentEntries.Count) current entries"
}

function Start-ReportViewer {
  [OutputType([bool])]
  param(
    [ValidateSet('sample', 'runtime')][string]$Report = 'runtime'
  )

  $serveScriptPath = Join-Path $PSScriptRoot 'serve_report.ps1'
  if (-not (Test-Path -LiteralPath $serveScriptPath -PathType Leaf -ErrorAction SilentlyContinue)) {
    Write-ParallelLog -Message "Cannot open report viewer because '$serveScriptPath' was not found." -Level Error -Target Both
    return $false
  }

  $pwshCommand = if ($PSVersionTable.PSEdition -ieq 'Core') { 'pwsh' } else { 'powershell' }
  $argList = @(
    '-NoProfile',
    '-ExecutionPolicy',
    'Bypass',
    '-File',
    $serveScriptPath,
    '-Report',
    $Report
  )

  try {
    Start-Process -FilePath $pwshCommand -ArgumentList $argList -WorkingDirectory $PSScriptRoot | Out-Null
    Write-ParallelLog -Message "Started report server helper using '$serveScriptPath' (mode=$Report)." -Target Both
    return $true
  }
  catch {
    Write-ParallelLog -Message "Failed to start report server helper '$serveScriptPath': $($_.Exception.Message)" -Level Error -Target Both
    return $false
  }
}

function Get-OutputPath {
  param(
    [System.IO.FileInfo]$FileInfo,
    [bool]$IsReprocess,
    [int]$JobId
  )

  $baseName = ConvertTo-BaseNameCodecTag -BaseName $FileInfo.BaseName.Trim()
  $ext = ".mp4"
  if ($FileInfo.Extension -ieq ".mkv") { $ext = ".mkv" }

  if ($IsReprocess) { return (Join-Path $processing_path "$baseName $JobId.temp$ext") }

  return (Join-Path $processing_path "$baseName - proc.$JobId.temp$ext")
}

function Get-FinalOutputName {
  param(
    [System.IO.FileInfo]$FileInfo,
    [bool]$IsReprocess
  )

  $baseName = ConvertTo-BaseNameCodecTag -BaseName $FileInfo.BaseName.Trim()
  $ext = ".mp4"
  if ($FileInfo.Extension -ieq ".mkv") { $ext = ".mkv" }

  if ($IsReprocess) {
    [int]$processCount = 0
    if ([int]::TryParse($baseName[$baseName.Length - 1], [ref]$processCount)) {
      $baseName = $baseName.Substring(0, $baseName.Length - 1).Trim()
    }

    return "$baseName $($processCount + 1)$ext"
  }

  return "$baseName - proc$ext"
}

function Get-NormalizedPath {
  [OutputType([string])]
  param([Parameter(Mandatory = $true)][string]$Path)

  try { $fullPath = [System.IO.Path]::GetFullPath($Path) }
  catch { $fullPath = $Path }

  return $fullPath.TrimEnd([System.IO.Path]::DirectorySeparatorChar, [System.IO.Path]::AltDirectorySeparatorChar)
}

function Test-PathIsUnder {
  [OutputType([bool])]
  param(
    [Parameter(Mandatory = $true)][string]$ChildPath,
    [Parameter(Mandatory = $true)][string]$ParentPath
  )

  $normalizedChild = Get-NormalizedPath -Path $ChildPath
  $normalizedParent = Get-NormalizedPath -Path $ParentPath

  $comparison = if ($IsWindows) { [System.StringComparison]::OrdinalIgnoreCase }
  else { [System.StringComparison]::Ordinal }

  if ([string]::Equals($normalizedChild, $normalizedParent, $comparison)) { return $true }

  $parentWithSeparator = $normalizedParent + [System.IO.Path]::DirectorySeparatorChar
  return $normalizedChild.StartsWith($parentWithSeparator, $comparison)
}

function Get-ArchiveDirectory {
  [OutputType([string])]
  param(
    [Parameter(Mandatory = $true)][string]$SourceDirectory,
    [Parameter(Mandatory = $true)][string]$MediaRoot,
    [Parameter(Mandatory = $true)][string]$ProcessedRoot
  )

  if (Test-PathIsUnder -ChildPath $SourceDirectory -ParentPath $MediaRoot) {
    $relativePath = [System.IO.Path]::GetRelativePath((Get-NormalizedPath -Path $MediaRoot), (Get-NormalizedPath -Path $SourceDirectory))
    if (($relativePath -eq '.') -or [string]::IsNullOrWhiteSpace($relativePath)) {
      return $ProcessedRoot
    }

    return Join-Path $ProcessedRoot $relativePath
  }

  # Fallback if source path is outside configured media root.
  return Join-Path $ProcessedRoot (Split-Path -Leaf $SourceDirectory)
}

function Get-InputItemList {
  param(
    [string]$InputPath,
    # When set, includes all known video formats (for audit/analyze mode).
    # When not set, only includes the narrower encoding-candidate subset.
    [switch]$AllMedia
  )

  $pathForScan = $InputPath
  $pathExistsLiteral = Test-Path -LiteralPath $pathForScan -ErrorAction SilentlyContinue
  $pathHasWildcard = [Management.Automation.WildcardPattern]::ContainsWildcardCharacters($pathForScan)

  $items = @()
  if ($pathExistsLiteral) {
    $items = Get-ChildItem -LiteralPath $pathForScan -Recurse -ErrorAction Stop
  }
  elseif ($pathHasWildcard) {
    $items = Get-ChildItem -Path $pathForScan -Recurse -ErrorAction Stop
  }
  else {
    throw "Cannot find path '$pathForScan' because it does not exist."
  }

  $reprocessFilter = { $_.Name -notmatch "- proc" }
  if ($CanReprocess) { $reprocessFilter = { $true } }

  $extensionFilter = if ($AllMedia) {
    # All known media file types. The library (derived from directory) identifies what it actually is.
    $allMediaExts = @(
      # Video
      '.avi', '.divx', '.mkv', '.mp4', '.m4v', '.m2ts', '.mts', '.mov', '.mpg', '.mpeg',
      '.ts', '.wmv', '.flv', '.webm', '.3gp', '.3g2', '.vob', '.f4v', '.ogv', '.rm', '.rmvb', '.asf',
      # Audio
      '.mp3', '.flac', '.aac', '.m4a', '.ogg', '.opus', '.wav', '.wma', '.aiff', '.aif',
      '.alac', '.ape', '.wv', '.mka', '.dts', '.ac3', '.eac3', '.dsf', '.dff',
      # Images
      '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.heic', '.heif',
      '.raw', '.cr2', '.cr3', '.nef', '.arw', '.dng', '.orf', '.rw2', '.pef',
      # Ebooks / documents
      '.epub', '.mobi', '.azw', '.azw3', '.pdf', '.cbz', '.cbr', '.cb7', '.djvu',
      # Subtitles / companion
      '.srt', '.ass', '.ssa', '.sub', '.idx', '.vtt', '.sup'
    )
    { $allMediaExts -contains $_.Extension.ToLowerInvariant() }.GetNewClosure()
  }
  else {
    { $_.Extension -match '^\.(avi|divx|m.*|ts|wmv)' }
  }

  return @(
    $items |
    Sort-Object $SortExpression |
    Where-Object -FilterScript $extensionFilter |
    Where-Object -FilterScript $reprocessFilter |
    Where-Object -FilterScript $UserFilter |
    Where-Object {
      if ($null -eq $LastRunDate) { return $true }
      return $_.CreationTime.Date -ge $LastRunDate.Date
    }
  )
}

function Get-ArrContext {
  param([System.IO.FileInfo]$SourceFile)

  if (Test-PathIsUnder -ChildPath $SourceFile.FullName -ParentPath $movies_path) {
    return @{
      Type    = [ArrType]::movie
      BaseUri = $radarr_baseUri
      ApiKey  = $radarr_apiKey
    }
  }

  if (Test-PathIsUnder -ChildPath $SourceFile.FullName -ParentPath $tv_shows_path) {
    return @{
      Type    = [ArrType]::series
      BaseUri = $sonarr_baseUri
      ApiKey  = $sonarr_apiKey
    }
  }

  return $null
}

function Invoke-ArrRefresh {
  param(
    [ArrType]$Type,
    [string]$TitleName,
    [string]$BaseUri,
    [string]$ApiKey,
    [int]$JobId
  )

  if ([string]::IsNullOrWhiteSpace($TitleName)) { return }

  $typeName = [ArrType].GetEnumName($Type)
  $normalizedTitle = $TitleName.Trim()
  $normalizedBaseUri = if ([string]::IsNullOrWhiteSpace($BaseUri)) { '' }
  else { $BaseUri.Trim().TrimEnd('/') }
  $refreshKey = "{0}|{1}|{2}" -f $typeName, $normalizedBaseUri, $normalizedTitle

  if (-not $script:ArrRefreshRequestedKeys.Add($refreshKey)) {
    Write-VerboseParallelLog -Message "Skipping duplicate Arr refresh request (type='$typeName', title='$normalizedTitle')." -JobId $JobId
    return
  }

  # Fire-and-forget: run the lookup + refresh in a background thread so it doesn't
  # block the main loop's progress rendering.
  $scriptBlock = {
    param($arrType, $titleName, $baseUri, $apiKey)
    $session = New-Object Microsoft.PowerShell.Commands.WebRequestSession
    $session.UserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

    # Look up title IDs
    $term = [System.Web.HttpUtility]::UrlEncode($titleName)
    $entity = $arrType
    $resp = Invoke-WebRequest -UseBasicParsing -Uri "$baseUri/api/v3/$entity/lookup?term=$term" -Method "GET" -WebSession $session -Headers @{ "Accept" = "*/*"; "X-Api-Key" = "$apiKey" } -ContentType "application/json" | ConvertFrom-Json | Where-Object { -not [string]::IsNullOrWhiteSpace($_.path) }
    $titleIds = @($resp.id)
    if (-not $titleIds -or $titleIds.Count -eq 0) { return }

    # Refresh
    if ($arrType -eq 'movie') {
      $body = "{`"name`":`"RefreshMovie`",`"movieIds`":[$($titleIds -join ',')]}"
    }
    else {
      $body = "{`"name`":`"RescanSeries`",`"seriesId`":$($titleIds[0])}"
    }
    Invoke-WebRequest -UseBasicParsing -Uri "$baseUri/api/v3/command" -Method "POST" -WebSession $session -Headers @{ "Accept" = "application/json, text/javascript, */*; q=0.01"; "X-Api-Key" = "$apiKey"; "X-Requested-With" = "XMLHttpRequest" } -ContentType "application/json" -Body "$body" | Out-Null
  }

  try {
    $job = Start-ThreadJob -ScriptBlock $scriptBlock -ArgumentList $typeName, $TitleName, $BaseUri, $ApiKey
    [void]$script:ArrRefreshJobs.Add($job)
    $script:ArrRefreshJobMeta[$job.Id] = [PSCustomObject]@{
      SourceJobId = $JobId
      TypeName    = $typeName
      TitleName   = $TitleName
    }
    Write-ParallelLog -Message "Arr refresh requested." -Target Both -JobId $JobId
  }
  catch {
    [void]$script:ArrRefreshRequestedKeys.Remove($refreshKey)
    Write-ParallelLog -Message "Arr refresh request failed: $($_.Exception.Message)" -Level Warning -Target Both -JobId $JobId
  }
}

function Enqueue-ArrRefreshTarget {
  param(
    [ArrType]$Type,
    [string]$TitleName,
    [string]$BaseUri,
    [string]$ApiKey,
    [int]$JobId
  )

  # Queue refresh targets during per-file finalization and dedupe by type/baseUri/title.
  # Actual Arr API refresh requests are dispatched once at end-of-run.
  if ([string]::IsNullOrWhiteSpace($TitleName)) { return }

  $typeName = [ArrType].GetEnumName($Type)
  $normalizedTitle = $TitleName.Trim()
  $normalizedBaseUri = if ([string]::IsNullOrWhiteSpace($BaseUri)) { '' }
  else { $BaseUri.Trim().TrimEnd('/') }
  $refreshKey = "{0}|{1}|{2}" -f $typeName, $normalizedBaseUri, $normalizedTitle

  if ($script:ArrRefreshTargets.ContainsKey($refreshKey)) {
    Write-VerboseParallelLog -Message "Skipping duplicate queued Arr refresh target (type='$typeName', title='$normalizedTitle')." -JobId $JobId
    return
  }

  $script:ArrRefreshTargets[$refreshKey] = [PSCustomObject]@{
    Type      = $Type
    TitleName = $normalizedTitle
    BaseUri   = $BaseUri
    ApiKey    = $ApiKey
    JobId     = $JobId
  }
}

function Invoke-QueuedArrRefreshes {
  param()

  # End-of-run dispatch ensures refresh happens after all files have been finalized.
  if (-not $RefreshArrOnCompletion) { return }
  if (-not $script:ArrRefreshTargets -or $script:ArrRefreshTargets.Count -eq 0) { return }

  Write-ParallelLog -Message "Dispatching end-of-run Arr refresh requests for $($script:ArrRefreshTargets.Count) deduped title(s)." -Target Both
  foreach ($target in @($script:ArrRefreshTargets.Values)) {
    Invoke-ArrRefresh -Type $target.Type -TitleName $target.TitleName -BaseUri $target.BaseUri -ApiKey $target.ApiKey -JobId $target.JobId
  }
}

function Start-TestProcess {
  param(
    [int]$JobId,
    [psobject]$JobState
  )

  $item = $JobState.Item
  $probe = $JobState.Probe

  if ($script:CancellationToken.IsCancellationRequested -or $script:StopRequested) {
    $JobState.State = "Skipped"
    $JobState.Status = "Canceled"
    return
  }

  $primaryVideoStream = @($probe.streams | Where-Object { $_.codec_type -ieq 'video' } | Select-Object -First 1)
  if (-not $primaryVideoStream) {
    $JobState.State = "Failed"
    $JobState.Error = "No video stream found for '$($item.FullName)'."
    Write-ParallelLog -Message $JobState.Error -Level Error -Target Both -JobId $JobId
    return
  }

  $analysis = $JobState.Analysis
  $audioMap = $analysis.AudioMap
  $subMap = $analysis.SubMap
  $audioDisp = $analysis.AudioDisp
  $subDisp = $analysis.SubDisp
  $scaleArg = $analysis.ScaleArg

  Write-VerboseParallelLog -Message "Job analysis selected. AudioMap='$audioMap' SubMap='$subMap' AudioDisp='$audioDisp' SubDisp='$subDisp' ScaleArg='$scaleArg'" -JobId $JobId

  $bitrateArg = ""
  if (-not [string]::IsNullOrWhiteSpace($BitrateControl)) { $bitrateArg = "-b:v $BitrateControl " }

  $progressTarget = "$($JobState.ProgressFile)"
  if ([string]::IsNullOrWhiteSpace($progressTarget)) { $progressTarget = "pipe:1" }

  if (-not $script:HWProfile) {
    $JobState.State = "Failed"
    $JobState.Error = "Hardware profile is not initialized."
    Write-ParallelLog -Message $JobState.Error -Level Error -Target Both -JobId $JobId
    return
  }

  $hwProfile = $script:HWProfile
  $hwaccelArgs = ''
  if (-not [string]::IsNullOrWhiteSpace($hwProfile.HwaccelArgs)) {
    $hwaccelArgs = "$($hwProfile.HwaccelArgs) "
  }
  $codecArgs = & $hwProfile.CodecArgs $script:CQPRateControlInt $bitrateArg

  $encodeProbeArgs = "-analyzeduration 4GB -probesize 4GB"
  $testProbeArgs = "-analyzeduration $($script:TestProbeAnalyzeDuration) -probesize $($script:TestProbeSize)"

  $ffmpegArgs = "-hide_banner -y -threads 0 ${hwaccelArgs}-copy_unknown $encodeProbeArgs "
  $ffmpegArgs += "-nostats -stats_period 0.25 -progress `"$progressTarget`" -i `"$($item.FullName)`" "
  $ffmpegArgs += "$scaleArg -default_mode infer_no_subs -map 0:v:0 $audioMap $subMap $audioDisp $subDisp -dn -map_metadata 0 -map_chapters 0 "
  $ffmpegArgs += "-c:v $($hwProfile.Encoder) -c:a copy -c:s copy "
  $ffmpegArgs += $codecArgs
  $ffmpegArgs += "-max_interleave_delta 500000 "
  $ffmpegArgs += "`"$($JobState.OutputFile)`""

  # Keep tests intentionally narrow: first video stream only, no audio/subtitles/data,
  # and a small fixed frame sample to validate encoder/hw options quickly.
  $testArgs = "-hide_banner -y -threads 0 ${hwaccelArgs}-copy_unknown $testProbeArgs "
  $testArgs += "-i `"$($item.FullName)`" "
  $testArgs += "$scaleArg -map 0:v:0 -an -sn -dn -map_metadata -1 -map_chapters -1 "
  $testArgs += "-c:v $($hwProfile.Encoder) "
  $testArgs += $codecArgs
  $testArgs += "-frames:v $($script:TestFrameCount) -f null $($script:NullDevice)"
  $fallbackSteps = @($hwProfile.FallbackChain)

  $sourceCodec = ''
  if ($analysis.PrimaryVideo -and $analysis.PrimaryVideo.codec_name) {
    $sourceCodec = "$($analysis.PrimaryVideo.codec_name)".Trim().ToLowerInvariant()
  }

  if (($sourceCodec -eq 'av1') -and ($hwProfile.Name -eq 'nvenc') -and (-not $script:CudaAv1DecodeSupportKnown)) {
    $script:CudaAv1DecodeSupported = Test-CudaAv1DecodeSupport -InputPath $item.FullName -JobId $JobId
    $script:CudaAv1DecodeSupportKnown = $true
  }

  # Heuristic: AV1 sources are a poor fit for initial CUDA hwaccel test variants on some systems.
  # Skip those test-chain entries and begin at a safer fallback so the capability check reflects
  # whether the final encode settings work, not whether AV1 CUDA decode startup is slow/failing.
  if (($sourceCodec -eq 'av1') -and ($hwProfile.Name -eq 'nvenc') -and (-not $script:CudaAv1DecodeSupported)) {
    $skippedCudaTestVariants = 0
    while (($testArgs -match '(?i)-hwaccel\s+cuda\b') -and ($fallbackSteps.Count -gt 0)) {
      $testArgs = & $fallbackSteps[0] $testArgs
      $ffmpegArgs = & $fallbackSteps[0] $ffmpegArgs
      $skippedCudaTestVariants++

      if ($fallbackSteps.Count -gt 1) {
        $fallbackSteps = @($fallbackSteps[1..($fallbackSteps.Count - 1)])
      }
      else {
        $fallbackSteps = @()
      }
    }

    if ($skippedCudaTestVariants -gt 0) {
      Write-ParallelLog -Message "AV1 heuristic: skipped $skippedCudaTestVariants CUDA decode test variant(s); starting test chain at a safer fallback." -Target Both -JobId $JobId
    }
  }

  $maxAttempts = [Math]::Max(1, $fallbackSteps.Count + 1)

  $JobState.TestFullArgs = $ffmpegArgs
  $JobState.TestArgs = $testArgs
  $JobState.TestAttempt = 1
  $JobState.TestMaxAttempts = $maxAttempts
  $JobState.TestFallbackSteps = $fallbackSteps

  Invoke-StartTestAttempt -JobId $JobId -JobState $JobState
}

function Invoke-StartTestAttempt {
  param(
    [int]$JobId,
    [psobject]$JobState
  )

  Write-VerboseParallelLog -Message "Running ffmpeg test attempt $($JobState.TestAttempt)/$($JobState.TestMaxAttempts) ($($script:HWProfile.Name)) with args: $($JobState.TestArgs)" -JobId $JobId

  $p = New-Object System.Diagnostics.Process
  $p.StartInfo.FileName = $script:ffmpeg_exe
  $p.StartInfo.Arguments = $JobState.TestArgs
  $p.StartInfo.UseShellExecute = $false
  $p.StartInfo.RedirectStandardOutput = $false
  $p.StartInfo.RedirectStandardError = $true
  $p.StartInfo.CreateNoWindow = $true

  $p.Start() | Out-Null
  Register-ChildProcess -Process $p -JobId $JobId
  $JobState.TestProcess = $p
  $JobState.TestStderrTask = $p.StandardError.ReadToEndAsync()
  $JobState.State = "Testing"
  $JobState.Status = "Testing ($($JobState.TestAttempt)/$($JobState.TestMaxAttempts))..."
}

function Start-EncodeProcess {
  [CmdletBinding(SupportsShouldProcess = $true)]
  param(
    [int]$JobId,
    [psobject]$JobState,
    [Parameter(Mandatory = $true)][string]$EffectiveArgs
  )

  $item = $JobState.Item
  $durationMs = $JobState.DurationMs

  if ($script:CancellationToken.IsCancellationRequested -or $script:StopRequested) {
    $JobState.State = "Skipped"
    $JobState.Status = "Canceled"
    return
  }

  $ffmpegArgs = $EffectiveArgs
  Write-VerboseParallelLog -Message "Launching ffmpeg with args: $ffmpegArgs" -JobId $JobId

  if ($script:ShowOutputCmdEnabled) {
    Write-ParallelLog -Message "Run Command: $($script:ffmpeg_exe) $ffmpegArgs" -Target Both -JobId $JobId
  }

  $p = New-Object System.Diagnostics.Process
  $p.StartInfo.FileName = $script:ffmpeg_exe
  $p.StartInfo.Arguments = $ffmpegArgs
  $p.StartInfo.UseShellExecute = $false
  $p.StartInfo.RedirectStandardOutput = $true
  $p.StartInfo.RedirectStandardError = $true
  $p.StartInfo.CreateNoWindow = $true

  if (-not $PSCmdlet.ShouldProcess($item.FullName, "Start ffmpeg encode")) { return }

  $started = $p.Start()
  if (-not $started) { throw "Failed to start ffmpeg process for $($item.FullName)" }
  Register-ChildProcess -Process $p -JobId $JobId

  $JobState.Process = $p
  $JobState.Started = Get-Date
  $JobState.DurationMs = $durationMs
  $JobState.State = "Running"
  $JobState.ProgressReadTask = $p.StandardOutput.ReadLineAsync()
  $JobState.StderrTask = $p.StandardError.ReadToEndAsync()

  # Open a per-job progress dump file for raw postmortem analysis.
  if ($LogVerbose) {
    $dumpName = "${script:LogPrefix}_progress_job${JobId}_$(Get-Date -Format 'yyyyMMddHHmmss')_$($PID).log"
    $dumpPath = Join-Path $script:log_path $dumpName
    try {
      $sw = [System.IO.StreamWriter]::new($dumpPath, $false, [System.Text.Encoding]::UTF8)
      $sw.AutoFlush = $true
      $sw.WriteLine("# Progress dump for Job-${JobId}: $($item.Name)")
      $sw.WriteLine("# DurationMs=$durationMs  Started=$(Get-Date -Format 'o')")
      $sw.WriteLine("# ---")
      $JobState.ProgressDumpWriter = $sw
    }
    catch {
      Write-VerboseParallelLog -Message "Could not create progress dump for Job-$JobId`: $($_.Exception.Message)" -JobId $JobId
    }
  }

  Write-ParallelLog -Message "Started encoding: $($item.Name)" -Target Both -JobId $JobId
}

function Update-ProgressState {
  [CmdletBinding(SupportsShouldProcess = $true)]
  param([hashtable]$StateTable)

  if (-not $PSCmdlet.ShouldProcess("Job state table", "Update ffmpeg progress state")) { return }

  # Poll jobs in the "Testing" state — asynchronously started test processes.
  foreach ($k in @($StateTable.Keys)) {
    $job = $StateTable[$k]
    if (-not $job) { continue }
    if ($job.State -ne "Testing") { continue }

    # TestProcess null means we are waiting out the retry backoff delay.
    if (-not $job.TestProcess) {
      if ((Get-Date) -ge $job.TestRetryAfter) {
        Invoke-StartTestAttempt -JobId $k -JobState $job
      }
      continue
    }

    if (-not $job.TestProcess.HasExited) { continue }

    $exitCode = $job.TestProcess.ExitCode
    $job.TestProcess = $null

    if ($exitCode -eq 0) {
      $effectiveArgs = $job.TestFullArgs
      Write-VerboseParallelLog -Message "ffmpeg test succeeded; effective encode args selected." -JobId $k
      Write-VerboseParallelLog -Message "Effective ffmpeg args: $effectiveArgs" -JobId $k
      $job.TestStderrTask = $null
      Start-EncodeProcess -JobId $k -JobState $job -EffectiveArgs $effectiveArgs
      continue
    }

    $err = ""
    try { $err = $job.TestStderrTask.GetAwaiter().GetResult().Trim() }
    catch { $err = "" }
    $job.TestStderrTask = $null
    $errSummary = "Unknown ffmpeg test error"
    if (-not [string]::IsNullOrWhiteSpace($err)) { $errSummary = ($err -split "`r?`n")[0].Trim() }

    Write-ParallelLog -Message "Test command failed (attempt $($job.TestAttempt)/$($job.TestMaxAttempts)): $errSummary" -Level Warning -Target Both -JobId $k

    $fallbackIndex = $job.TestAttempt - 1
    if ($fallbackIndex -ge $job.TestFallbackSteps.Count) {
      $job.State = "Failed"
      $job.Status = "Failed (test exhausted)"
      $job.Error = "ffmpeg test command failed after $($job.TestMaxAttempts) attempts for encoder '$($script:HWProfile.Name)'."
      Write-ParallelLog -Message $job.Error -Level Error -Target Both -JobId $k
      continue
    }

    try {
      $job.TestArgs = & $job.TestFallbackSteps[$fallbackIndex] $job.TestArgs
      $job.TestFullArgs = & $job.TestFallbackSteps[$fallbackIndex] $job.TestFullArgs
      Write-ParallelLog -Message "Applying fallback step $($fallbackIndex + 1)/$($job.TestFallbackSteps.Count) for encoder '$($script:HWProfile.Name)'." -Level Warning -Target Both -JobId $k
    }
    catch {
      Write-ParallelLog -Message "Fallback step $($fallbackIndex + 1) failed: $($_.Exception.Message)" -Level Warning -Target Both -JobId $k
    }

    $sleepSec = [Math]::Pow(2, $job.TestAttempt)
    $job.TestRetryAfter = (Get-Date).AddSeconds($sleepSec)
    $job.TestAttempt++
    # TestProcess stays null; next tick will start the next attempt once delay elapses.
  }

  # Only the host loop mutates terminal job states; workers update process/progress artifacts.
  foreach ($k in @($StateTable.Keys)) {
    $job = $StateTable[$k]
    if (-not $job) { continue }
    if ($job.State -ne "Running") { continue }

    if (-not ($job.ProgressScratch -is [hashtable])) { $job.ProgressScratch = @{} }

    # Process each complete progress block immediately (like the serial script)
    # so that PreviousProgress tracks every intermediate value and progress never
    # appears to jump when multiple blocks have buffered between render cycles.
    while ($job.ProgressReadTask -and $job.ProgressReadTask.IsCompleted) {
      $line = $job.ProgressReadTask.GetAwaiter().GetResult()
      if ($null -eq $line) {
        $job.ProgressReadTask = $null
        break
      }

      # Write raw line with timestamp for postmortem analysis.
      if ($job.ProgressDumpWriter) {
        try { $job.ProgressDumpWriter.WriteLine("$(Get-Date -Format 'HH:mm:ss.fff') $line") }
        catch { $null = $_ }
      }

      $update = Read-FFmpegProgressLine -Line $line -ProgressState $job.ProgressScratch
      if ($update) {
        $outMs = 0.0
        $parsedOut = [TimeSpan]::Zero
        $outTimeValue = 0.0
        $outSource = 'none'

        # Prefer out_time parsing for stable progress; fall back to out_time_us then out_time_ms.
        if ([TimeSpan]::TryParse("$($update.out_time)", [ref]$parsedOut) -and ($parsedOut.TotalMilliseconds -gt 0)) {
          $outMs = $parsedOut.TotalMilliseconds
          $outSource = 'out_time'
        }
        elseif ([decimal]::TryParse("$($update.out_time_us)", [ref]$outTimeValue) -and ($outTimeValue -gt 0)) {
          # out_time_us is always microseconds.
          $outMs = ($outTimeValue / 1000.0)
          $outSource = 'out_time_us'
        }
        elseif ([decimal]::TryParse("$($update.out_time_ms)", [ref]$outTimeValue) -and ($outTimeValue -gt 0)) {
          # ffmpeg may emit out_time_ms as microseconds despite the key name.
          $outMsRaw = $outTimeValue
          $outMsFromUs = ($outTimeValue / 1000.0)
          $outMs = if ($outMsRaw -gt $job.DurationMs) { $outMsFromUs }
          else { $outMsRaw }
          $outSource = 'out_time_ms'
        }

        if ($LogVerbose -and (-not $job.ProgressDiagLogged)) {
          # Log the very first progress block per job so we can diagnose unit issues.
          Write-VerboseParallelLog -Message "First progress block: out_time='$($update.out_time)' out_time_us='$($update.out_time_us)' out_time_ms='$($update.out_time_ms)' frame='$($update.frame)' speed='$($update.speed)' -> outMs=$outMs source=$outSource durationMs=$($job.DurationMs) estFrames=$($job.EstimatedTotalFrames)" -JobId $k
          $job.ProgressDiagLogged = $true
        }

        # Compute both time-based and frame-based progress, then use the higher one.
        # The MKV muxer can stall (out_time stuck) while the encoder keeps producing
        # frames, so frame-based progress keeps the display moving during muxer stalls.
        $timePct = [decimal]-1.0
        $framePct = [decimal]-1.0

        if (($outMs -gt 0) -and ($job.DurationMs -gt 0)) {
          $timePct = [Math]::Min(100.0, ($outMs / $job.DurationMs) * 100.0)
        }

        if ($job.EstimatedTotalFrames -gt 0) {
          $frameCount = 0.0
          if ([decimal]::TryParse("$($update.frame)", [ref]$frameCount) -and ($frameCount -gt 0)) {
            $framePct = [Math]::Min(100.0, ($frameCount / $job.EstimatedTotalFrames) * 100.0)
          }
        }

        $progressDecimal = [Math]::Max($timePct, $framePct)

        if ($progressDecimal -gt 0) {
          if ($progressDecimal -lt $job.PreviousProgress) {
            if ($LogVerbose) {
              Write-VerboseParallelLog -Message "Progress reset detected. previous=$($job.PreviousProgress), current=$progressDecimal" -JobId $k
            }
            $job.PreviousProgress = 0.0
          }

          if ($progressDecimal -gt $job.PreviousProgress) {
            $job.Percent = $progressDecimal
            $job.PreviousProgress = $progressDecimal
          }
        }

        if ($update.speed) { $job.Speed = $update.speed.Trim() }

        $displaySpeed = "0.00"
        if (-not [string]::IsNullOrWhiteSpace($job.Speed)) { $displaySpeed = $job.Speed }
        $job.Status = "{0:0.0}% {1}x" -f $job.Percent, $displaySpeed
      }

      if (-not $job.Process.HasExited) {
        $job.ProgressReadTask = $job.Process.StandardOutput.ReadLineAsync()
      }
      else { $job.ProgressReadTask = $null }
    }

    if ($job.Process.HasExited) {
      $job.ExitCode = $job.Process.ExitCode
      $job.Ended = Get-Date

      # Close progress dump writer now that the process has exited.
      if ($job.ProgressDumpWriter) {
        try {
          $job.ProgressDumpWriter.WriteLine("# --- Process exited at $(Get-Date -Format 'o') ExitCode=$($job.ExitCode)")
          $job.ProgressDumpWriter.Dispose()
        }
        catch { $null = $_ }
        $job.ProgressDumpWriter = $null
      }

      if ($job.ExitCode -eq 0) {
        $job.State = "Completed"
        $job.Percent = 100.0
        $job.Status = "Completed"
        Write-ParallelLog -Message "Completed encoding: $($job.Item.Name)" -Target Both -JobId $k
      }
      else {
        $job.State = "Failed"
        $job.Status = "Failed (ExitCode=$($job.ExitCode))"
        $stderr = ""
        if ($job.StderrTask) {
          try { $stderr = $job.StderrTask.GetAwaiter().GetResult() }
          catch { $stderr = "" }
        }
        if (-not [string]::IsNullOrWhiteSpace($stderr)) {
          $job.Error = ($stderr -split "`r?`n" | Select-Object -First 1)
        }
        Write-ParallelLog -Message "Encode failed: $($job.Item.Name). ExitCode=$($job.ExitCode). $($job.Error)" -Level Error -Target Both -JobId $k
      }
    }
  }
}

function Show-ParallelProgress {
  param([hashtable]$StateTable, [int]$TotalCount)

  $completed = @($StateTable.Values | Where-Object { $_.State -in @("Completed", "Failed", "Skipped") }).Count
  $running = @($StateTable.Values | Where-Object { $_.State -in @("Running", "Testing") }).Count
  $scanned = [Math]::Min($TotalCount, [Math]::Max(0, $script:ScanProcessedCount))
  $scanPercent = if ($TotalCount -gt 0) { [Math]::Min(100, [int](($scanned / [decimal]$TotalCount) * 100.0)) }
  else { 0 }

  $lastScanName = $script:LastScannedFile
  if ([string]::IsNullOrWhiteSpace($lastScanName)) {
    $lastScanName = "(waiting for first candidate)"
  }

  if ($lastScanName.Length -gt 90) {
    $lastScanName = $lastScanName.Substring(0, 87) + "..."
  }

  $pulseFrames = @('.', 'o', 'O', 'o')
  $pulse = $pulseFrames[[Math]::Floor([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 500) % $pulseFrames.Count]
  $parentStatus = "Scanned $scanned/$TotalCount | Running $running | Completed $completed | Last: $lastScanName"

  if (-not $script:CanRenderProgress) {
    $now = Get-Date
    if (($now - $script:LastTextProgressUpdate).TotalSeconds -ge 1.0) {
      $script:LastTextProgressUpdate = $now
      $parentActivity = if ($running -gt 0) { "ffmpeg workload [$pulse]" }
      else { "Scanning media candidates [$pulse]" }
      Write-Host ("{0} | {1}" -f $now.ToString("HH:mm:ss"), "$parentActivity | $parentStatus")
    }
    return
  }

  $renderNow = Get-Date
  if (($renderNow - $script:LastProgressRender).TotalSeconds -lt 0.25) { return }
  $script:LastProgressRender = $renderNow

  $parentActivity = if ($running -gt 0) { "ffmpeg workload [$pulse]" }
  else { "Scanning media candidates [$pulse]" }

  Write-Progress -Id 0 -Activity $parentActivity -Status $parentStatus -PercentComplete $scanPercent

  foreach ($key in @($StateTable.Keys | Sort-Object)) {
    $job = $StateTable[$key]
    if (-not $job) { continue }

    if ($job.State -in @('Completed', 'Failed', 'Skipped')) {
      if (-not $job.ProgressClosed) {
        Write-Progress -Id $key -Completed
        $job.ProgressClosed = $true
      }
      continue
    }

    $job.ProgressClosed = $false

    $activity = "Loading:   "
    switch ($job.State) {
      'Testing' { $activity = "Testing    [$pulse]:" }
      'Running' { $activity = "Processing [$pulse]:" }
      'Failed' { $activity = 'Failed:    ' }
      'Skipped' { $activity = 'Skipped:   ' }
    }

    $title = $job.Item.Name
    if ([string]::IsNullOrWhiteSpace($title)) { $title = "Job-$key" }

    $pctFloat = [Math]::Max(0.0, [Math]::Min(100.0, [decimal]$job.Percent))
    $pctInt = [int]$pctFloat
    if ($pctFloat -ge 100.0) { $pctText = ' 100%' }
    else { $pctText = ("{0,4:0.0}%" -f $pctFloat) }
    $speedText = "0.00"
    if (-not [string]::IsNullOrWhiteSpace($job.Speed)) { $speedText = $job.Speed.Trim() }

    # ffmpeg already reports speed with a trailing 'x' (for example 2.35x);
    # normalize it so we never render duplicate suffixes like '2.35xx'.
    $speedText = ($speedText -replace '\s*[xX]+\s*$', '')
    if ([string]::IsNullOrWhiteSpace($speedText)) { $speedText = "0.00" }

    $speedValue = 0.0
    if ([decimal]::TryParse($speedText, [ref]$speedValue)) {
      # Use fixed 2-decimal width to reduce status jitter while values fluctuate.
      $speedText = ("{0:0.00}" -f $speedValue).PadLeft(5)
    }
    else {
      if ($speedText -match '^(?i:n/?a)$') { $speedText = 'N/A' }
      if ($speedText.Length -gt 5) { $speedText = $speedText.Substring(0, 5) }

      # Keep non-numeric values (for example N/A) the same width as numeric speed values.
      $speedText = $speedText.PadLeft(5)
    }

    $leftPrefix = "$pctText $($speedText)x | "
    $bufferWidth = [Math]::Max(70, [Math]::Min([console]::BufferWidth, 160))
    $maxTitleLength = [Math]::Max(8, ($bufferWidth - $leftPrefix.Length - 10))
    if ($title.Length -gt $maxTitleLength) {
      $title = $title.Substring(0, ($maxTitleLength - 3)) + "..."
    }

    $statusText = "$leftPrefix$title"

    Write-Progress -Id $key -Activity $activity -Status $statusText -PercentComplete $pctInt
  }
}

function Complete-JobFinalization {
  param(
    [int]$JobId,
    [psobject]$JobState,
    [hashtable]$StateTable,
    [int]$TotalCount
  )

  if ($JobState.State -ne "Completed") { return }

  if (-not $MoveOnCompletion) { return }

  $queueLockReleased = $false
  $releaseQueueLock = {
    if ($queueLockReleased) { return }
    if ($JobState -and $JobState.Item -and (-not [string]::IsNullOrWhiteSpace($JobState.Item.FullName))) {
      Unregister-QueuedFileMutex -FilePath $JobState.Item.FullName
      $queueLockReleased = $true
    }
  }

  try {
    $srcFile = $JobState.Item
    $JobState.Status = "Verifying output..."
    Update-ProgressState -StateTable $StateTable
    Show-ParallelProgress -StateTable $StateTable -TotalCount $TotalCount

    if (-not (Test-Path -LiteralPath $JobState.OutputFile)) {
      $JobState.State = "Failed"
      $JobState.Status = "Failed (Missing output)"
      Write-ParallelLog -Message "Post-process failed: output file missing at '$($JobState.OutputFile)'" -Level Error -Target Both -JobId $JobId
      return
    }

    $newFileLength = (Get-Item -LiteralPath $JobState.OutputFile).Length
    if ($newFileLength -le 100KB) {
      $JobState.State = "Failed"
      $JobState.Status = "Failed (Output too small)"
      Write-ParallelLog -Message "Destination file is too small; deleting output." -Level Error -Target Both -JobId $JobId
      Remove-Item -LiteralPath $JobState.OutputFile -Force -ErrorAction SilentlyContinue
      return
    }

    if (($JobState.InputSizeBytes -le $newFileLength) -and [string]::IsNullOrWhiteSpace($ResizeResolution) -and (-not $ForceConvert)) {
      $JobState.Status = "Output larger than source; cleaning up..."
      Write-ParallelLog -Message "Output file is larger than source and -ForceConvert is not set; deleting output." -Level Warning -Target Both -JobId $JobId
      Remove-Item -LiteralPath $JobState.OutputFile -Force -ErrorAction SilentlyContinue

      if ($MoveOnCompletion) {
        $updatedBase = ConvertTo-BaseNameCodecTag -BaseName $srcFile.BaseName.Trim()
        $renameTarget = if ($JobState.IsReprocess) { "$updatedBase$($srcFile.Extension)" }
        else { "$updatedBase - proc$($srcFile.Extension)" }

        if ($srcFile.Name -ne $renameTarget) {
          $JobState.Status = "Renaming source..."
          Rename-Item -LiteralPath $srcFile.FullName -NewName $renameTarget -ErrorAction SilentlyContinue
          Write-ParallelLog -Message "Renamed source after larger-output rejection: $renameTarget" -Target Both -JobId $JobId
        }
      }

      & $releaseQueueLock

      if ($RefreshArrOnCompletion) {
        $JobState.Status = "Refreshing Arr..."
        $arr = Get-ArrContext -SourceFile $srcFile
        if (-not $arr) {
          Write-ParallelLog -Message "Skipping Arr refresh: source path '$($srcFile.FullName)' is outside configured movies_path '$movies_path' and tv_shows_path '$tv_shows_path'." -Level Warning -Target Both -JobId $JobId
        }
        else {
          $titleName = (($arr.Type -eq [ArrType]::series -or $srcFile.Directory.Name.ToLower().StartsWith("season")) ? $srcFile.Directory.Parent.Name : $srcFile.Directory.Name)
          Enqueue-ArrRefreshTarget -Type $arr.Type -TitleName $titleName -BaseUri $arr.BaseUri -ApiKey $arr.ApiKey -JobId $JobId
        }
      }

      $JobState.Status = "Completed (output larger)"
      return
    }

    $fullProcessedPath = Get-ArchiveDirectory -SourceDirectory $srcFile.DirectoryName -MediaRoot $media_path -ProcessedRoot $processed_path
    Write-ParallelLog -Message "Finalize paths:`n output='$($JobState.OutputFile)'`n final='$($JobState.FinalOutputFile)'`n archive='$fullProcessedPath'`n source='$($srcFile.FullName)'" -Target Log -JobId $JobId

    if (-not (Test-Path -LiteralPath $fullProcessedPath)) {
      New-Item -Path $fullProcessedPath -ItemType Directory | Out-Null
    }

    $JobState.Status = "Moving files..."
    Update-ProgressState -StateTable $StateTable
    Show-ParallelProgress -StateTable $StateTable -TotalCount $TotalCount
    Move-Item -LiteralPath $JobState.OutputFile -Destination $JobState.FinalOutputFile -Force
    Move-Item -LiteralPath $srcFile.FullName -Destination $fullProcessedPath -Force
    Write-ParallelLog -Message "Moved output to: '$($JobState.FinalOutputFile)'`n Archived source to: '$fullProcessedPath'." -Target Both -JobId $JobId

    & $releaseQueueLock

    if ($RefreshArrOnCompletion) {
      $JobState.Status = "Refreshing Arr..."
      $arr = Get-ArrContext -SourceFile $srcFile
      if (-not $arr) {
        Write-ParallelLog -Message "Skipping Arr refresh: source path '$($srcFile.FullName)' is outside configured movies_path '$movies_path' and tv_shows_path '$tv_shows_path'." -Level Warning -Target Both -JobId $JobId
      }
      else {
        $titleName = (($arr.Type -eq [ArrType]::series -or $srcFile.Directory.Name.ToLower().StartsWith("season")) ? $srcFile.Directory.Parent.Name : $srcFile.Directory.Name)
        Enqueue-ArrRefreshTarget -Type $arr.Type -TitleName $titleName -BaseUri $arr.BaseUri -ApiKey $arr.ApiKey -JobId $JobId
      }
    }

    $JobState.Status = "Completed"
  }
  catch {
    Write-ParallelLog -Message "Post-process move failed: $($_.Exception.Message) | output='$($JobState.OutputFile)' final='$($JobState.FinalOutputFile)' source='$($JobState.Item.FullName)'" -Level Warning -Target Both -JobId $JobId
  }
}

function Get-QueuedWorkItem {
  param(
    [System.IO.FileInfo]$Item,
    [int]$JobId
  )

  $script:ScanProcessedCount++
  $script:LastScannedFile = $Item.Name

  if ($script:CancellationToken.IsCancellationRequested -or $script:StopRequested) { return $null }

  $isReprocess = ($Item.Name -match "- proc")
  $fileStatus = Get-InputFileStatus -FilePath $Item.FullName -SkipLock:$SkipFileLock
  Write-ParallelLog -Message "$fileStatus $($Item.Name)" -Target Log
  if ($fileStatus -in @('[L]', '[NF]', '[E]')) {
    Write-ParallelLog -Message "Skipping due to file status $fileStatus" -Level Warning -Target Both
    return $null
  }

  $probe = Get-FFprobeJson -InputPath $Item.FullName
  if (-not $probe) {
    Write-ParallelLog -Message "Skipping '$($Item.FullName)': ffprobe returned no metadata." -Level Warning -Target Both
    return $null
  }

  $durationMs = Get-ProbeDuration -Probe $probe
  $itdm = [Math]::Max(1, [int][Math]::Floor(($durationMs / 1000.0) / 60.0))
  $ismb = [int][Math]::Floor($Item.Length / 1MB)
  $impm = [int][Math]::Ceiling(($ismb / [decimal]$itdm))
  $isMovieItem = Test-PathIsUnder -ChildPath $Item.FullName -ParentPath $movies_path

  if ((-not $ForceConvert) -and (((-not $isMovieItem) -and ($ismb -le 350)) -or ($isMovieItem -and ($ismb -le 900)))) {
    Write-VerboseParallelLog -Message "Skipping threshold floor: ismb=$ismb itdm=$itdm impm=$impm movie=$isMovieItem" -JobId $JobId
    return $null
  }

  $srcResolutionWidth = 0
  $srcResolutionHeight = 0
  $primaryVideo = @($probe.streams | Where-Object { $_.codec_type -ieq 'video' } | Select-Object -First 1)
  if ($primaryVideo) {
    [void][int]::TryParse("$($primaryVideo.Width)", [ref]$srcResolutionWidth)
    [void][int]::TryParse("$($primaryVideo.Height)", [ref]$srcResolutionHeight)
  }

  $srcResType = Get-ResTypeFromDimension -ResW $srcResolutionWidth -ResH $srcResolutionHeight
  $tmpm = 0

  if ((-not $ForceConvert) -and (-not $ForceResize)) {
    if ($srcResType -ne [ResTypes]::UNKNOWN) {
      switch ($srcResType) {
        ([ResTypes]::SD) { $tmpm = 5 }
        ([ResTypes]::DVD) { $tmpm = 7 }
        ([ResTypes]::HD) { $tmpm = 15 }
        ([ResTypes]::FHD) { $tmpm = 20 }
        ([ResTypes]::QHD) { $tmpm = 30 }
        ([ResTypes]::UHD_4K) { $tmpm = 40 }
        default { $tmpm = 27 }
      }

      if ($impm -le $tmpm) {
        Write-VerboseParallelLog -Message "Skipping, threshold not met: impm=$impm tmpm=$tmpm size=${ismb}MB time=${itdm}min" -JobId $JobId
        return $null
      }

      if ($isMovieItem -and (
          (($srcResType -eq [ResTypes]::SD) -and (($ismb * 1MB) -lt 0.8GB)) -or
          (($srcResType -eq [ResTypes]::HD) -and (($ismb * 1MB) -lt 1.5GB)) -or
          (($srcResType -eq [ResTypes]::HD) -and (($ismb * 1MB) -lt 3.5GB)) -or
          (($srcResType -eq [ResTypes]::FHD) -and (($ismb * 1MB) -lt 5GB)) -or
          (($srcResType -eq [ResTypes]::UHD_4K) -and (($ismb * 1MB) -lt 9GB))
        )) {
        Write-VerboseParallelLog -Message "Skipping movie size floor: resType=$srcResType size=${ismb}MB" -JobId $JobId
        return $null
      }
    }
    else {
      if ($impm -le 27) {
        Write-VerboseParallelLog -Message "Skipping unknown-res threshold: impm=$impm <= 27" -JobId $JobId
        return $null
      }
    }
  }

  $analysis = Get-EncodeAnalysis -Probe $probe
  $streamSummary = "video=$(@($probe.streams | Where-Object { $_.codec_type -ieq 'video' }).Count) audio=$(@($probe.streams | Where-Object { $_.codec_type -ieq 'audio' }).Count) sub=$(@($probe.streams | Where-Object { $_.codec_type -ieq 'subtitle' }).Count)"
  Write-VerboseParallelLog -Message "Preflight analysis for '$($Item.Name)': $streamSummary maps=[audio:$($analysis.AudioMap)] [sub:$($analysis.SubMap)] scale='$($analysis.ScaleArg)'" -JobId $JobId

  $outputFile = Get-OutputPath -FileInfo $Item -IsReprocess $isReprocess -JobId $JobId
  $finalOutputName = Get-FinalOutputName -FileInfo $Item -IsReprocess $isReprocess
  $finalOutputFile = Join-Path $Item.DirectoryName $finalOutputName

  if ((Test-Path -LiteralPath $finalOutputFile) -and (-not $ForceConvert)) {
    Write-ParallelLog -Message "Skipping '$($Item.Name)': final output exists and -ForceConvert not set." -Target Both
    return $null
  }

  $work = [PSCustomObject]@{
    Id                   = $JobId
    Item                 = $Item
    Probe                = $probe
    DurationMs           = $durationMs
    EstimatedTotalFrames = 0.0
    OutputFile           = $outputFile
    FinalOutputFile      = $finalOutputFile
    FinalOutputName      = $finalOutputName
    InputSizeBytes       = $Item.Length
    IsReprocess          = $isReprocess
    Analysis             = $analysis
    ProgressFile         = "pipe:1"
    LastReadPosition     = 0L
    ProgressScratch      = @{}
    ProgressReadTask     = $null
    Process              = $null
    StderrTask           = $null
    Percent              = 0
    Speed                = "0.00"
    Status               = "Queued"
    State                = "Queued"
    ExitCode             = -1
    Error                = ""
    Started              = $null
    Ended                = $null
    ProgressClosed       = $false
    Finalized            = $false
    PreviousProgress     = 0.0
    ProgressDiagLogged   = $false
    ProgressDumpWriter   = $null
    TestProcess          = $null
    TestStderrTask       = $null
    TestArgs             = ""
    TestFullArgs         = ""
    TestAttempt          = 0
    TestMaxAttempts      = 0
    TestFallbackSteps    = @()
    TestRetryAfter       = [DateTime]::MinValue
    QueueLockName        = ""
    QueueLockFile        = ""

  }

  Write-ParallelLog -Message "Queued: $($Item.Name)" -Target Log -JobId $JobId

  # Estimate total frame count from video stream frame rate and duration.
  # Used as a progress fallback when the muxer hasn't flushed yet (out_time=N/A).
  $videoStream = @($probe.streams | Where-Object { $_.codec_type -ieq 'video' } | Select-Object -First 1)
  if ($videoStream -and $videoStream.r_frame_rate -match '^(\d+)/(\d+)$') {
    $fpsNum = [decimal]$Matches[1]
    $fpsDen = [decimal]$Matches[2]
    if ($fpsDen -gt 0) {
      $fps = $fpsNum / $fpsDen
      $work.EstimatedTotalFrames = [Math]::Max(1.0, $fps * ($durationMs / 1000.0))
    }
  }

  return $work
}

function Stop-ParallelExecution {
  [CmdletBinding(SupportsShouldProcess = $true)]
  [OutputType([int])]
  param(
    [hashtable]$StateTable,
    [System.Collections.Generic.Queue[object]]$PendingQueue,
    [System.Collections.Generic.Queue[object]]$CandidateQueue,
    [string]$Reason
  )

  if (-not $PSCmdlet.ShouldProcess("Parallel encoding run", "Stop execution and cancel pending work")) { return 0 }

  $script:StopRequested = $true
  $canceledPending = 0

  # Drain pending first so no new work is started while active processes are being stopped.
  if ($PendingQueue) {
    while ($PendingQueue.Count -gt 0) {
      $pendingItem = $PendingQueue.Dequeue()
      if ($pendingItem -and $pendingItem.Item -and (-not [string]::IsNullOrWhiteSpace($pendingItem.Item.FullName))) {
        Unregister-QueuedFileMutex -FilePath $pendingItem.Item.FullName
      }
      $canceledPending++
    }
  }

  if ($CandidateQueue) {
    while ($CandidateQueue.Count -gt 0) {
      [void]$CandidateQueue.Dequeue()
      $canceledPending++
    }
  }

  if ($StateTable) {
    # Running and Testing processes are terminated best-effort; final state is reconciled in the host loop.
    foreach ($active in @($StateTable.Values | Where-Object { $_.State -in @('Running', 'Testing') })) {
      # Kill encode process (Running state).
      if ($active.Process -and (-not $active.Process.HasExited)) {
        $killed = $false
        try { $active.Process.Kill(); $killed = $true }
        catch {
          Write-ParallelLog -Message "Unable to stop active ffmpeg process cleanly." -Level Warning -Target Log
        }

        if ($killed) {
          try { $active.Process.WaitForExit(5000) }  # timeout guard
          catch {
            Write-VerboseParallelLog -Message "Exception while waiting for ffmpeg process to exit: $($_.Exception.Message)" -JobId $active.Id
          }
          $active.Status = "Canceled"
        }
      }

      # Kill test process (Testing state).
      if ($active.TestProcess -and (-not $active.TestProcess.HasExited)) {
        try { $active.TestProcess.Kill() } catch { }
        try { $active.TestProcess.WaitForExit(5000) } catch { }
        $active.TestProcess = $null
        $active.Status = "Canceled"
      }
    }
  }

  Write-ParallelLog -Message "Shutdown requested: $Reason. Pending canceled=$canceledPending" -Level Warning -Target Both
  return $canceledPending
}

function Register-CancellationHandler {
  [CmdletBinding()]
  param()

  if (-not $script:CancellationSource) {
    $script:CancellationSource = [System.Threading.CancellationTokenSource]::new()
    $script:CancellationToken = $script:CancellationSource.Token
  }

  if (-not $script:EngineExitSubscription) {
    $script:EngineExitSubscription = Register-EngineEvent -SourceIdentifier PowerShell.Exiting -Action {
      $script:StopRequested = $true

      if ($script:CancellationSource -and (-not $script:CancellationSource.IsCancellationRequested)) {
        try { $script:CancellationSource.Cancel() }
        catch {
          Write-Verbose "Cancellation request during engine-exit handling failed: $($_.Exception.Message)"
        }
      }

      # Best-effort process cleanup for host close/abort scenarios.
      if ($script:StateTableRef) {
        foreach ($active in @($script:StateTableRef.Values | Where-Object { $_ -and ($_.State -in @('Running', 'Testing')) })) {
          if ($active.Process -and (-not $active.Process.HasExited)) {
            $killed = $false
            try { $active.Process.Kill(); $killed = $true }
            catch {
              Write-Verbose "Failed to kill ffmpeg process during engine-exit handling: $($_.Exception.Message)"
            }

            if ($killed) {
              try { $active.Process.WaitForExit(5000) }  # timeout guard
              catch {
                Write-VerboseParallelLog -Message "Exception while waiting for ffmpeg process to exit: $($_.Exception.Message)" -JobId $active.Id
              }
            }
          }

          if ($active.TestProcess -and (-not $active.TestProcess.HasExited)) {
            try { $active.TestProcess.Kill() } catch { }
            try { $active.TestProcess.WaitForExit(5000) } catch { }
          }
        }
      }

      # Release queue locks promptly during engine shutdown/abort handling.
      if ($script:QueuedFileMutexes) {
        foreach ($key in @($script:QueuedFileMutexes.Keys)) {
          $entry = $script:QueuedFileMutexes[$key]
          if ($entry) {
            if ($entry.LockFile -and (Test-Path -LiteralPath $entry.LockFile -PathType Leaf -ErrorAction SilentlyContinue)) {
              try { Remove-Item -LiteralPath $entry.LockFile -Force -ErrorAction SilentlyContinue } catch { }
            }
            if ($entry.Mutex) {
              try { [void]$entry.Mutex.ReleaseMutex() } catch { }
              try { $entry.Mutex.Dispose() } catch { }
            }
          }
        }
        $script:QueuedFileMutexes.Clear()
      }
    }
  }
}

function Unregister-CancellationHandler {
  [CmdletBinding()]
  param()

  if ($script:EngineExitSubscription) {
    try { Unregister-Event -SubscriptionId $script:EngineExitSubscription.Id -ErrorAction SilentlyContinue }
    catch {
      Write-VerboseParallelLog -Message "Unable to unregister PowerShell.Exiting event cleanly: $($_.Exception.Message)"
    }

    try { Remove-Job -Id $script:EngineExitSubscription.Id -Force -ErrorAction SilentlyContinue }
    catch {
      Write-VerboseParallelLog -Message "Unable to remove event job cleanly: $($_.Exception.Message)"
    }

    $script:EngineExitSubscription = $null
  }

  if ($script:CancellationSource) {
    try {
      if (-not $script:CancellationSource.IsCancellationRequested) {
        $script:CancellationSource.Cancel()
      }
    }
    catch {
      Write-VerboseParallelLog -Message "Unable to cancel token source cleanly: $($_.Exception.Message)"
    }

    try { $script:CancellationSource.Dispose() }
    catch {
      Write-VerboseParallelLog -Message "Unable to dispose cancellation token source cleanly: $($_.Exception.Message)"
    }
    $script:CancellationSource = $null
    $script:CancellationToken = [System.Threading.CancellationToken]::None
  }
}

$sync = $null
$pending = $null
$candidates = $null
$canceledPendingCount = 0
$runStarted = Get-Date
Initialize-ChildProcessContainment
Start-ChildProcessWatchdog
Register-CancellationHandler

# Clean up old log files (main logs and progress dumps).
$logFilter = Join-Path $script:log_path "${script:LogPrefix}_*.log"
$progressFilter = Join-Path $script:log_path "${script:LogPrefix}_progress_job*.log"
$oldLogs = @(Get-Item -Path $logFilter, $progressFilter -ErrorAction SilentlyContinue | Where-Object {
    $_.FullName -ne $script:logFilePath -and $_.CreationTime -lt (Get-Date).AddMinutes(-60)
  })
if ($oldLogs.Count -gt 0) {
  Write-ParallelLog -Message "Cleaning up $($oldLogs.Count) old log file(s)..." -Target Both
  $removedCount = 0
  foreach ($log in $oldLogs) {
    # Use file timestamps instead of name parsing so cleanup works regardless of
    # script prefix shape (for example names that already contain underscores).
    $isOld = $log.CreationTime -lt (Get-Date).AddDays(-1)
    if (-not $isOld) {
      $isOld = $log.LastWriteTime -lt (Get-Date).AddDays(-1)
    }

    if ($isOld -or ($log.Length -lt 2KB)) {
      Remove-Item -LiteralPath $log.FullName -Force -ErrorAction SilentlyContinue
      $removedCount++
    }
  }
  if ($removedCount -gt 0) {
    Write-ParallelLog -Message "Removed $removedCount old log file(s)." -Target Both
  }
}

$hasInputPath = -not [string]::IsNullOrWhiteSpace($Path)

if (($Analyze -or (-not $Compact -and -not $ViewReport)) -and -not $hasInputPath) {
  Write-ParallelLog -Message "-Path is required for encode and analyze runs." -Level Error -Target Both
  exit 1
}

if ($ViewReport -and -not $Analyze -and -not $Compact) {
  if (-not (Start-ReportViewer -Report 'runtime')) {
    exit 1
  }

  exit 0
}

# Handle -Analyze and -Compact modes
if ($Analyze -or $Compact) {
  # Keep analysis artifacts with encode logs so all run outputs are in one place.
  $outputDir = $script:log_path
  $inProgressNdjsonPath = Join-Path $outputDir 'metadata.ndjson.inprogress'
  if (-not (Test-Path $outputDir)) {
    Write-ParallelLog -Message "Output directory does not exist: $outputDir" -Level Error
    exit 1
  }

  Write-ParallelLog -Message "Analysis artifacts directory: '$outputDir'" -Target Both

  Remove-AnalyzeTempArtifacts -OutputDir $outputDir

  # Mutex for parallel runs
  $mutex = $null
  try {
    $mutex = [System.Threading.Mutex]::new($false, "Global\ffmpeg_h265_analyze_$([System.IO.Path]::GetFileName($outputDir))")
    if (-not $mutex.WaitOne(0)) {
      Write-ParallelLog -Message "Another -Analyze run is already in progress for this directory" -Level Error
      exit 1
    }
  }
  catch {
    Write-ParallelLog -Message "Failed to acquire mutex: $($_.Exception.Message)" -Level Error
    exit 1
  }

  try {
    if ($Compact) {
      $configText = ($script:RunConfig.GetEnumerator() | ForEach-Object { "{0}='{1}'" -f $_.Key, $_.Value }) -join "; "
      Write-ParallelLog -Message "Start compaction run. $configText" -Target Log
      Write-ParallelLog -Message "Starting compaction mode" -Target Both
      Invoke-CompactMetadata -OutputDir $outputDir
      if (Test-Path -LiteralPath $inProgressNdjsonPath -PathType Leaf -ErrorAction SilentlyContinue) {
        Remove-Item -LiteralPath $inProgressNdjsonPath -Force -ErrorAction SilentlyContinue
      }
      Unregister-AnalyzeTempArtifact -Path $inProgressNdjsonPath
      Remove-AnalyzeTempArtifacts -OutputDir $outputDir
      Write-ParallelLog -Message "Compaction completed" -Target Both

      if ($ViewReport) {
        if (-not (Start-ReportViewer -Report 'runtime')) {
          exit 1
        }
      }

      exit 0
    }

    # Analyze mode
    $configText = ($script:RunConfig.GetEnumerator() | ForEach-Object { "{0}='{1}'" -f $_.Key, $_.Value }) -join "; "
    Write-ParallelLog -Message "Start analysis run. $configText" -Target Log
    Write-ParallelLog -Message "Analyze hash strategy: '$HashAlgorithm'." -Target Both
    Write-ParallelLog -Message "Starting analysis mode" -Target Both
    $items = Get-InputItemList -InputPath $Path -AllMedia
    if (-not $items -or $items.Count -eq 0) {
      Write-ParallelLog -Message "No matching files found for analysis" -Target Both
      exit 0
    }

    Write-ParallelLog -Message "Discovered $($items.Count) file(s) for analysis" -Target Both

    $metadataList = @()
    Register-AnalyzeTempArtifact -Path $inProgressNdjsonPath
    if (Test-Path -LiteralPath $inProgressNdjsonPath -PathType Leaf -ErrorAction SilentlyContinue) {
      Remove-Item -LiteralPath $inProgressNdjsonPath -Force -ErrorAction SilentlyContinue
    }

    $analysisStartedAt = Get-Date
    $analysisWorkMs = 0.0
    $processed = 0
    foreach ($item in $items) {
      if (Test-AnalyzeCancellationRequested) {
        Write-ParallelLog -Message "Analyze cancellation requested. Stopping scan before writing outputs." -Level Warning -Target Both
        break
      }

      $processed++
      Write-Progress -Activity "Analyzing files" -Status "$processed/$($items.Count): $($item.Name)" -PercentComplete (($processed / $items.Count) * 100)

      $itemStartedAt = Get-Date
      $meta = Get-FileMetadata -FilePath $item.FullName -HashAlgorithm $HashAlgorithm
      $itemElapsedMs = ((Get-Date) - $itemStartedAt).TotalMilliseconds
      $analysisWorkMs += $itemElapsedMs
      if ($meta) {
        $metadataList += $meta

        try {
          $line = $meta | ConvertTo-Json -Compress -Depth 10
          [System.IO.File]::AppendAllText($inProgressNdjsonPath, $line + [Environment]::NewLine, [System.Text.Encoding]::UTF8)
        }
        catch {
          Write-ParallelLog -Message "Failed to update analysis in-progress output '$inProgressNdjsonPath': $($_.Exception.Message)" -Level Warning -Target Both
        }

        Write-ParallelLog -Message "Analyzed [$processed/$($items.Count)] in $([Math]::Round(($itemElapsedMs / 1000.0), 3))s: $($item.FullName)" -Target Log
      }
    }

    Write-Progress -Completed

    if (Test-AnalyzeCancellationRequested) {
      Remove-AnalyzeTempArtifacts -OutputDir $outputDir
      Write-ParallelLog -Message "Analyze canceled. Temporary artifacts were cleaned up." -Level Warning -Target Both
      exit 130
    }

    if ($metadataList.Count -gt 0) {
      Export-MetadataToNDJSON -MetadataList $metadataList -OutputDir $outputDir -GenerateHtml
      if (Test-AnalyzeCancellationRequested) {
        Remove-AnalyzeTempArtifacts -OutputDir $outputDir
        Write-ParallelLog -Message "Analyze canceled during output generation. Temporary artifacts were cleaned up." -Level Warning -Target Both
        exit 130
      }

      Invoke-CompactMetadata -OutputDir $outputDir
      Remove-AnalyzeTempArtifacts -OutputDir $outputDir

      if (Test-AnalyzeCancellationRequested) {
        Write-ParallelLog -Message "Analyze canceled during compaction. Temporary artifacts were cleaned up." -Level Warning -Target Both
        exit 130
      }

      $analysisElapsed = (Get-Date) - $analysisStartedAt
      $count = [Math]::Max(1, $metadataList.Count)
      $avgSecondsPerFile = [Math]::Round((($analysisWorkMs / $count) / 1000.0), 3)
      $filesPerMinute = [Math]::Round(($metadataList.Count / [Math]::Max(0.001, $analysisElapsed.TotalMinutes)), 2)
      Write-ParallelLog -Message "Analysis timing summary: total=$([Math]::Round($analysisElapsed.TotalMinutes, 2)) min; avg/file=$avgSecondsPerFile s; throughput=$filesPerMinute files/min; files=$($metadataList.Count)." -Target Both
      Write-ParallelLog -Message "Analysis completed: processed $($metadataList.Count) files" -Target Both
    }
    else {
      Write-ParallelLog -Message "No valid metadata collected" -Target Both
    }
  }
  finally {
    Remove-AnalyzeTempArtifacts -OutputDir $outputDir
    if ($mutex) {
      try { $mutex.ReleaseMutex() } catch { }
      try { $mutex.Dispose() } catch { }
    }
  }

  if ($ViewReport) {
    if (-not (Start-ReportViewer -Report 'runtime')) {
      exit 1
    }
  }

  exit 0
}

try {
  $script:HWProfile = Get-HardwareEncoderProfile -PreferredEncoder $Encoder

  $configText = ($script:RunConfig.GetEnumerator() | ForEach-Object { "{0}='{1}'" -f $_.Key, $_.Value }) -join "; "
  Write-ParallelLog -Message "Start encoding run. $configText" -Target Log

  $items = Get-InputItemList -InputPath $Path
  if (-not $items -or $items.Count -eq 0) {
    Write-ParallelLog -Message "No matching files found." -Target Both
    return
  }

  Write-ParallelLog -Message "Discovered $($items.Count) file(s). MaxParallelJobs=$MaxParallelJobs" -Target Both

  if (-not (Test-Path -LiteralPath $processing_path)) {
    New-Item -Path $processing_path -ItemType Directory | Out-Null
  }

  Remove-StaleQueuedFileLockMetadata -ProcessingRoot $processing_path

  # Shared synchronized state is owned by the host loop and read for progress rendering.
  $sync = [System.Collections.Hashtable]::Synchronized(@{})
  $pending = New-Object System.Collections.Generic.Queue[object]
  $candidates = New-Object System.Collections.Generic.Queue[object]
  $script:StateTableRef = $sync
  $script:PendingQueueRef = $pending
  $script:CandidateQueueRef = $candidates

  foreach ($item in $items) { $candidates.Enqueue($item) }

  $nextId = 1
  $totalKnown = $items.Count
  $script:TotalWorkCount = $totalKnown
  if ($totalKnown -eq 0) {
    Write-ParallelLog -Message "No queued work after preflight checks." -Target Both
    return
  }

  while (($candidates.Count -gt 0) -or ($pending.Count -gt 0) -or (@($sync.Values | Where-Object { $_.State -in @('Running', 'Testing') }).Count -gt 0)) {
    if ($script:CancellationToken.IsCancellationRequested -and (-not $script:StopRequested)) {
      $canceledPendingCount += Stop-ParallelExecution -StateTable $sync -PendingQueue $pending -CandidateQueue $candidates -Reason "Cancellation requested"
    }

    if ($script:StopRequested) { break }

    # Only triage when we need to refill capacity. Avoid ffprobe blocking while all encoders are busy.
    $runningBeforeTriage = @($sync.Values | Where-Object { $_.State -in @('Running', 'Testing') }).Count
    $needRefill = (($runningBeforeTriage + $pending.Count) -lt $MaxParallelJobs)
    $triageBudget = if ($runningBeforeTriage -gt 0) { 2 }
    else { [Math]::Max(6, ($MaxParallelJobs * 3)) }

    $triagedThisTick = 0

    while ($needRefill -and ($pending.Count -lt $MaxParallelJobs) -and ($candidates.Count -gt 0) -and ($triagedThisTick -lt $triageBudget)) {
      if ($script:StopRequested -or $script:CancellationToken.IsCancellationRequested) { break }

      $candidate = $candidates.Dequeue()
      $work = Get-QueuedWorkItem -Item $candidate -JobId $nextId
      if ($work) {
        if ($script:QueuedFileMutexes.ContainsKey($work.Item.FullName)) {
          Write-VerboseParallelLog -Message "Skipping duplicate queue candidate in current run: '$($work.Item.FullName)'"
          $triagedThisTick++
          $needRefill = (($runningBeforeTriage + $pending.Count) -lt $MaxParallelJobs)
          continue
        }

        $lockEntry = Register-QueuedFileMutex -FilePath $work.Item.FullName -ProcessingRoot $processing_path
        if ($lockEntry) {
          $script:QueuedFileMutexes[$work.Item.FullName] = $lockEntry
          $work.QueueLockName = $lockEntry.LockName
          $work.QueueLockFile = $lockEntry.LockFile
          $pending.Enqueue($work)
          $nextId++
        }
        else {
          Write-ParallelLog -Message "Skipping '$($work.Item.FullName)': queue lock already held by another active run." -Level Warning -Target Both
        }
      }

      $triagedThisTick++
      $needRefill = (($runningBeforeTriage + $pending.Count) -lt $MaxParallelJobs)  # runningBeforeTriage includes Testing
    }

    # Admission control: never exceed MaxParallelJobs active (Testing + Running) slots.
    while ((@($sync.Values | Where-Object { $_.State -in @('Running', 'Testing') }).Count -lt $MaxParallelJobs) -and ($pending.Count -gt 0)) {
      if ($script:StopRequested -or $script:CancellationToken.IsCancellationRequested) { break }

      $work = $pending.Dequeue()
      $sync[$work.Id] = $work
      Start-TestProcess -JobId $work.Id -JobState $sync[$work.Id]
    }

    Update-ProgressState -StateTable $sync
    Show-ParallelProgress -StateTable $sync -TotalCount $totalKnown

    if ($LogVerbose) {
      $now = Get-Date
      if (($now - $script:LastVerboseSnapshot).TotalSeconds -ge 5.0) {
        $script:LastVerboseSnapshot = $now
        $runningCount = @($sync.Values | Where-Object { $_.State -in @('Running', 'Testing') }).Count
        $completedCount = @($sync.Values | Where-Object { $_.State -in @('Completed', 'Failed', 'Skipped') }).Count
        $pendingCount = $pending.Count + $candidates.Count
        $jobProgressParts = @()
        foreach ($jk in @($sync.Keys | Sort-Object)) {
          $jv = $sync[$jk]
          if ($jv -and ($jv.State -in @('Running', 'Testing'))) {
            $hasTask = ($null -ne $jv.ProgressReadTask)
            $taskDone = ($hasTask -and $jv.ProgressReadTask.IsCompleted)
            $jobProgressParts += "J$jk=$($jv.Percent)%/spd=$($jv.Speed)/task=$hasTask/done=$taskDone"
          }
        }
        $jobDetail = if ($jobProgressParts.Count -gt 0) { " jobs=[" + ($jobProgressParts -join '; ') + "]" }
        else { '' }

        Write-VerboseParallelLog -Message "Queue snapshot: running=$runningCount completed=$completedCount pending=$pendingCount total=$totalKnown$jobDetail"
      }
    }

    foreach ($k in @($sync.Keys)) {
      $job = $sync[$k]
      if (-not $job) { continue }
      if ($job.State -eq "Completed" -and (-not $job.Finalized)) {
        Complete-JobFinalization -JobId $k -JobState $job -StateTable $sync -TotalCount $totalKnown
        $job.Finalized = $true
      }
      if ($job.State -eq "Failed" -and $ExitOnError) {
        Write-ParallelLog -Message "ExitOnError requested; stopping remaining work." -Level Error -Target Both
        $canceledPendingCount += Stop-ParallelExecution -StateTable $sync -PendingQueue $pending -CandidateQueue $candidates -Reason "ExitOnError after job failure"
        break
      }
    }

    Start-Sleep -Milliseconds 300
  }

  # Finalize any remaining jobs that completed on the last loop iteration.
  foreach ($k in @($sync.Keys | Sort-Object)) {
    $job = $sync[$k]
    if (-not $job) { continue }
    if ($job.State -eq "Completed" -and (-not $job.Finalized)) {
      Complete-JobFinalization -JobId $k -JobState $job -StateTable $sync -TotalCount $totalKnown
      $job.Finalized = $true
    }
  }

  Invoke-QueuedArrRefreshes

  if ($script:CanRenderProgress) {
    Write-Progress -Id 0 -Activity "ffmpeg workload" -Completed
    foreach ($k in @($sync.Keys)) {
      Write-Progress -Id $k -ParentId 0 -Activity "[Job-$k]" -Completed
    }
  }

  $success = @($sync.Values | Where-Object { $_.State -eq 'Completed' }).Count
  $failed = @($sync.Values | Where-Object { $_.State -eq 'Failed' }).Count
  $skipped = [Math]::Max(0, $items.Count - $sync.Count) + $canceledPendingCount
  $elapsed = New-TimeSpan -Start $runStarted -End (Get-Date)

  Write-ParallelLog -Message "Processing completed. Success=$success Failed=$failed SkippedOrCanceled=$skipped Elapsed=$($elapsed.ToString('hh\:mm\:ss'))" -Target Both

  foreach ($failedJob in @($sync.Values | Where-Object { $_.State -eq 'Failed' } | Sort-Object Id)) {
    $firstError = "No error line captured"
    if (-not [string]::IsNullOrWhiteSpace($failedJob.Error)) {
      $firstError = $failedJob.Error.Trim()
    }

    Write-ParallelLog -Message "Failed item: $($failedJob.Item.FullName) | FirstError: $firstError" -Level Error -Target Both -JobId $failedJob.Id
  }

}
catch {
  if ($sync -or $pending -or $candidates) {
    $canceledPendingCount += Stop-ParallelExecution -StateTable $sync -PendingQueue $pending -CandidateQueue $candidates -Reason "Unhandled exception"
  }
  Write-ParallelLog -Message $_.Exception.Message -Level Error -Target Both
  Write-ParallelLog -Message $_.ScriptStackTrace -Level Error -Target Log
  throw
}
finally {
  # Ctrl+C may bypass the normal scheduler shutdown path; enforce child-process cleanup here.
  if ($sync) {
    foreach ($active in @($sync.Values | Where-Object { $_ -and ($_.State -in @('Running', 'Testing')) })) {
      if ($active.Process -and (-not $active.Process.HasExited)) {
        $killed = $false
        try { $active.Process.Kill(); $killed = $true }
        catch {
          Write-VerboseParallelLog -Message "Final cleanup could not stop ffmpeg process: $($_.Exception.Message)"
        }

        if ($killed) {
          try { $active.Process.WaitForExit(5000) }  # timeout guard
          catch {
            Write-VerboseParallelLog -Message "Exception while waiting for ffmpeg process to exit: $($_.Exception.Message)" -JobId $active.Id
          }
          $active.State = 'Skipped'
          $active.Status = 'Canceled'
        }
      }

      if ($active.TestProcess -and (-not $active.TestProcess.HasExited)) {
        $testKilled = $false
        try { $active.TestProcess.Kill(); $testKilled = $true }
        catch {
          Write-VerboseParallelLog -Message "Final cleanup could not stop ffmpeg test process: $($_.Exception.Message)"
        }

        if ($testKilled) {
          try { $active.TestProcess.WaitForExit(5000) } catch { }
          $active.TestProcess = $null
          $active.State = 'Skipped'
          $active.Status = 'Canceled'
        }
      }

      if ($active.ProgressDumpWriter) {
        try {
          $active.ProgressDumpWriter.WriteLine("# --- Cleanup at $(Get-Date -Format 'o')")
          $active.ProgressDumpWriter.Dispose()
        }
        catch { $null = $_ }

        $active.ProgressDumpWriter = $null
      }
    }
  }

  if ($script:CanRenderProgress) {
    try { Write-Progress -Id 0 -Activity "ffmpeg workload" -Completed }
    catch {
      if ($LogVerbose) {
        Write-Verbose "Unable to clear parent progress bar cleanly: $($_.Exception.Message)"
      }
    }
  }

  if ($null -ne $script:PreviousProgressView) {
    try { $PSStyle.Progress.View = $script:PreviousProgressView }
    catch {
      if ($LogVerbose) {
        Write-Verbose "Unable to restore previous progress view: $($_.Exception.Message)"
      }
    }
  }

  if ($script:ArrRefreshJobs -and $script:ArrRefreshJobs.Count -gt 0) {
    $arrJobs = @($script:ArrRefreshJobs)
    $arrJobs | Wait-Job -Timeout $script:ArrRefreshTimeoutSeconds | Out-Null

    foreach ($arrJob in $arrJobs) {
      $meta = $null
      if ($script:ArrRefreshJobMeta.ContainsKey($arrJob.Id)) {
        $meta = $script:ArrRefreshJobMeta[$arrJob.Id]
      }

      $sourceJobId = 0
      $arrTypeName = 'unknown'
      $arrTitle = 'unknown'
      if ($meta) {
        $sourceJobId = $meta.SourceJobId
        $arrTypeName = $meta.TypeName
        $arrTitle = $meta.TitleName
      }

      if ($arrJob.State -eq 'Completed') {
        try {
          $null = Receive-Job -Job $arrJob -Keep -ErrorAction Stop
          Write-ParallelLog -Message "Arr refresh completed (type='$arrTypeName', title='$arrTitle')." -Target Both -JobId $sourceJobId
        }
        catch {
          Write-ParallelLog -Message "Arr refresh failed (type='$arrTypeName', title='$arrTitle'): $($_.Exception.Message)" -Level Warning -Target Both -JobId $sourceJobId
        }
      }
      elseif ($arrJob.State -eq 'Failed') {
        $reason = ''
        if ($arrJob.ChildJobs -and $arrJob.ChildJobs.Count -gt 0 -and $arrJob.ChildJobs[0].JobStateInfo.Reason) {
          $reason = $arrJob.ChildJobs[0].JobStateInfo.Reason.Message
        }
        if ([string]::IsNullOrWhiteSpace($reason)) { $reason = 'Thread job failed.' }
        Write-ParallelLog -Message "Arr refresh failed (type='$arrTypeName', title='$arrTitle'): $reason" -Level Warning -Target Both -JobId $sourceJobId
      }
      else {
        Write-ParallelLog -Message "Arr refresh did not complete before shutdown timeout (state=$($arrJob.State), type='$arrTypeName', title='$arrTitle')." -Level Warning -Target Both -JobId $sourceJobId
      }
    }

    $arrJobs | Remove-Job -Force -ErrorAction SilentlyContinue
    $script:ArrRefreshJobs.Clear()
    $script:ArrRefreshJobMeta.Clear()
    $script:ArrRefreshRequestedKeys.Clear()
  }

  if ($script:ArrRefreshTargets) {
    $script:ArrRefreshTargets.Clear()
  }

  if ($script:ChildWatchdogProcess -and (-not $script:ChildWatchdogProcess.HasExited)) {
    try { Stop-Process -Id $script:ChildWatchdogProcess.Id -Force -ErrorAction SilentlyContinue }
    catch {
      Write-VerboseParallelLog -Message "Unable to stop child process watchdog cleanly: $($_.Exception.Message)"
    }
    finally {
      $script:ChildWatchdogProcess = $null
    }
  }

  if ($script:ProcessJobHandle -ne [IntPtr]::Zero) {
    try { [void][ffmpegH265.JobObjectNative]::CloseHandle($script:ProcessJobHandle) }
    catch {
      Write-VerboseParallelLog -Message "Unable to close process containment job handle cleanly: $($_.Exception.Message)"
    }
    finally {
      $script:ProcessJobHandle = [IntPtr]::Zero
      $script:ProcessJobEnabled = $false
    }
  }

  if ($script:LogMutex) {
    try { $script:LogMutex.Dispose() }
    catch {
      if ($LogVerbose) {
        Write-Verbose "Unable to dispose log mutex cleanly: $($_.Exception.Message)"
      }
    }
    $script:LogMutex = $null
  }

  Unregister-QueuedFileMutexes

  $script:StateTableRef = $null
  $script:PendingQueueRef = $null
  $script:CandidateQueueRef = $null
  $script:TotalWorkCount = 0
  Unregister-CancellationHandler
}
