<#
.SYNOPSIS
  Parallel ffmpeg + NVENC conversion with bounded concurrency and per-job progress.
.DESCRIPTION
  Discovers files, precomputes encode work items, and runs up to -MaxParallelJobs ffmpeg
  processes concurrently. Uses a synchronized hashtable for progress state and renders
  a parent + per-job Write-Progress display. Logging is job-scoped and thread-safe.
#>

param(
  # Runtime-scope parameters belong here (scan behavior, filters, encoding, logging toggles).
  # Environment/deployment settings (paths, endpoints, API keys) are intentionally
  # resolved from env/config to avoid a duplicated parameter surface.
  [Parameter(Mandatory = $true)][string] $Path,
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
  [Parameter(Mandatory = $false)][string] $AudioLang = "eng",
  [Parameter(Mandatory = $false)][string] $SubLang = "eng",
  [Parameter(Mandatory = $false)][datetime] $LastRunDate,
  [Parameter(Mandatory = $false)][switch] $SkipFileLock = $false,
  [Parameter(Mandatory = $false, HelpMessage = "Directory containing ffmpeg_nvenc_h265.config.json")][string] $ConfigPath = ""
)

$ShowProgress = -not $NoProgress.IsPresent
$script:CanRenderProgress = $ShowProgress
$MoveOnCompletion = -not $SkipMoveOnCompletion.IsPresent
$RefreshArrOnCompletion = -not $SkipArrRefresh.IsPresent
$script:LogEnabled = $LogEnabled.IsPresent
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

if ($PSVersionTable.PSVersion -lt [version]'7.5.0') {
  throw "This script requires PowerShell 7.5 or newer. Current version: $($PSVersionTable.PSVersion)"
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
  AudioLang            = $AudioLang
  SubLang              = $SubLang
  LastRunDate          = $LastRunDate
  SkipFileLock         = $SkipFileLock.IsPresent
  ConfigPath           = $ConfigPath
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
  Write-Host "Warning: MaxParallelJobs=$MaxParallelJobs may exceed practical NVENC concurrency on many GPUs." -ForegroundColor Yellow
}

$script:ScriptRoot = $PSScriptRoot
if (-not $script:ScriptRoot) {
  $script:ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
}

$script:logFileName = "ffmpeg_nvenc_h265_$(Get-Date -Format "yyyyMMddHHmmssffff").log"
$script:logFilePath = $null
$script:LogMutexName = if ($IsWindows) { "Global\ffmpeg_nvenc_h265_log" }
else { "ffmpeg_nvenc_h265_log" }
$script:LogMutex = $null

# Configuration values are required, resolved as env var first, then config file.
# Precedence for environment/deployment settings is:
# 1) Environment variable
# 2) Config file value
# 3) Fail fast (for required keys)
function Get-ConfigValue {
  param(
    [Parameter(Mandatory = $true)][string]$ConfigKey,
    [Parameter(Mandatory = $true)][string]$EnvVarName,
    [Parameter(Mandatory = $true)][object]$ConfigData
  )

  $value = [Environment]::GetEnvironmentVariable($EnvVarName)

  if ([string]::IsNullOrWhiteSpace([string]$value) -and $ConfigData -and $ConfigData.PSObject.Properties[$ConfigKey]) {
    $value = $ConfigData.$ConfigKey
  }

  if ([string]::IsNullOrWhiteSpace([string]$value)) {
    throw "Required setting '$ConfigKey' is missing. Set '$ConfigKey' in ffmpeg_nvenc_h265.config.json or environment variable '$EnvVarName'."
  }

  return $value
}

function Get-OptionalConfigValue {
  param(
    [Parameter(Mandatory = $true)][string]$ConfigKey,
    [Parameter(Mandatory = $true)][string]$EnvVarName,
    [Parameter(Mandatory = $true)][object]$ConfigData
  )

  $value = [Environment]::GetEnvironmentVariable($EnvVarName)

  if ([string]::IsNullOrWhiteSpace([string]$value) -and $ConfigData -and $ConfigData.PSObject.Properties[$ConfigKey]) {
    $value = $ConfigData.$ConfigKey
  }

  return $value
}

function Resolve-DiscoveredLogPath {
  param(
    [Parameter(Mandatory = $true)][string[]]$SearchLocations,
    [Parameter(Mandatory = $false)][string]$LoadedConfigDirectory
  )

  if (-not [string]::IsNullOrWhiteSpace($LoadedConfigDirectory)) {
    return $LoadedConfigDirectory
  }

  foreach ($location in $SearchLocations) {
    if ([string]::IsNullOrWhiteSpace($location)) { continue }
    if (Test-Path -LiteralPath $location -PathType Container -ErrorAction SilentlyContinue) {
      return $location
    }
  }

  return $PWD.Path
}

$configData = $null
$configLocations = @()
$loadedConfigDirectory = $null

if (-not [string]::IsNullOrWhiteSpace($ConfigPath)) {
  # Explicit ConfigPath overrides all discovery
  $configLocations = @($ConfigPath)
}
else {
  # Auto-discovery: current dir, Documents (including OneDrive), script root
  $configLocations = @(
    $PWD.Path,
    [Environment]::GetFolderPath('MyDocuments'),
    $script:ScriptRoot
  )
}

foreach ($location in $configLocations) {
  if ([string]::IsNullOrWhiteSpace($location)) { continue }
  
  $configPath = Join-Path $location "ffmpeg_nvenc_h265.config.json"
  if (Test-Path -LiteralPath $configPath) {
    try {
      $configData = Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json
      $loadedConfigDirectory = Split-Path -Parent $configPath
      Write-Verbose "Loaded configuration from: $configPath"
      break
    }
    catch {
      Write-Warning "Failed to load config from '$configPath': $($_.Exception.Message)"
    }
  }
}

$script:ffmpeg_path = Get-ConfigValue -ConfigKey "ffmpeg_path" -EnvVarName "FFMPEG_PATH" -ConfigData $configData
$script:log_path = Get-OptionalConfigValue -ConfigKey "log_path" -EnvVarName "FFMPEG_LOG_PATH" -ConfigData $configData
if ([string]::IsNullOrWhiteSpace([string]$script:log_path)) {
  $script:log_path = Resolve-DiscoveredLogPath -SearchLocations $configLocations -LoadedConfigDirectory $loadedConfigDirectory
}
$processed_path = Get-ConfigValue -ConfigKey "processed_path" -EnvVarName "FFMPEG_PROCESSED_PATH" -ConfigData $configData
$processing_path = Get-ConfigValue -ConfigKey "processing_path" -EnvVarName "FFMPEG_PROCESSING_PATH" -ConfigData $configData
$media_path = Get-ConfigValue -ConfigKey "media_path" -EnvVarName "FFMPEG_MEDIA_PATH" -ConfigData $configData
$moviesSubfolder = Get-OptionalConfigValue -ConfigKey "movies_subfolder" -EnvVarName "FFMPEG_MOVIES_SUBFOLDER" -ConfigData $configData
$tvShowsSubfolder = Get-OptionalConfigValue -ConfigKey "tv_shows_subfolder" -EnvVarName "FFMPEG_TV_SHOWS_SUBFOLDER" -ConfigData $configData
if ([string]::IsNullOrWhiteSpace([string]$moviesSubfolder)) { $moviesSubfolder = "Movies" }
if ([string]::IsNullOrWhiteSpace([string]$tvShowsSubfolder)) { $tvShowsSubfolder = "TV Shows" }
$tv_shows_path = Join-Path $media_path $tvShowsSubfolder
$movies_path = Join-Path $media_path $moviesSubfolder
$radarr_baseUri = Get-ConfigValue -ConfigKey "radarr_baseUri" -EnvVarName "RADARR_BASE_URI" -ConfigData $configData
$radarr_apiKey = Get-ConfigValue -ConfigKey "radarr_apiKey" -EnvVarName "RADARR_API_KEY" -ConfigData $configData
$sonarr_baseUri = Get-ConfigValue -ConfigKey "sonarr_baseUri" -EnvVarName "SONARR_BASE_URI" -ConfigData $configData
$sonarr_apiKey = Get-ConfigValue -ConfigKey "sonarr_apiKey" -EnvVarName "SONARR_API_KEY" -ConfigData $configData

$script:NullDevice = if ($IsWindows) { 'NUL' }
else { '/dev/null' }

$ffmpegExeName = if ($IsWindows) { 'ffmpeg.exe' }
else { 'ffmpeg' }
$ffprobeExeName = if ($IsWindows) { 'ffprobe.exe' }
else { 'ffprobe' }

if ((Split-Path -Path $script:ffmpeg_path -Leaf) -ieq $ffmpegExeName) {
  $script:ffmpeg_exe = $script:ffmpeg_path
}
else {
  $script:ffmpeg_exe = Join-Path $script:ffmpeg_path $ffmpegExeName
}

if ((Split-Path -Path $script:ffmpeg_path -Leaf) -ieq $ffprobeExeName) {
  $script:ffprobe_exe = $script:ffmpeg_path
}
elseif ((Split-Path -Path $script:ffmpeg_path -Leaf) -ieq $ffmpegExeName) {
  $script:ffprobe_exe = Join-Path (Split-Path -Parent $script:ffmpeg_path) $ffprobeExeName
}
else {
  $script:ffprobe_exe = Join-Path $script:ffmpeg_path $ffprobeExeName
}

if (-not (Test-Path -LiteralPath $script:ffmpeg_exe)) {
  throw "ffmpeg executable was not found at '$($script:ffmpeg_exe)'. Set ffmpeg_path (or FFMPEG_PATH) to the ffmpeg directory or full executable path."
}

if (-not (Test-Path -LiteralPath $script:ffprobe_exe)) {
  throw "ffprobe executable was not found at '$($script:ffprobe_exe)'. Set ffmpeg_path (or FFMPEG_PATH) to the ffmpeg directory or full executable path."
}

if (-not (Test-Path -LiteralPath $script:log_path)) {
  New-Item -Path $script:log_path -ItemType Directory -Force | Out-Null
}

$script:logFilePath = Join-Path $script:log_path $script:logFileName

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
    finally { if ($lockAcquired) { [void]$script:LogMutex.ReleaseMutex() }}
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
  param([string]$InputPath)

  if ($script:StopRequested -or $script:CancellationToken.IsCancellationRequested) { return $null }

  $ffprobeExe = $script:ffprobe_exe
  $ffprobeArgs = "`"$InputPath`" -v quiet -hide_banner -analyzeduration 4GB -probesize 4GB -show_format -show_streams -print_format json -sexagesimal"

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
  if ((-not [int]::TryParse($parts[0], [ref]$w)) -or (-not [int]::TryParse($parts[1], [ref]$h))) { return "" }

  if ($RetainAspectValue) { return "-vf scale_cuda=${w}:-1:interp_algo=lanczos" }

  return "-vf scale_cuda=${w}:${h}:interp_algo=lanczos"
}

function Get-TargetResolution {
  param([string]$ResizeValue)

  if ([string]::IsNullOrWhiteSpace($ResizeValue)) { return $null }

  $parts = @($ResizeValue.Split(':') | ForEach-Object { $_.Trim() })
  if ($parts.Count -ne 2) { return $null }

  $w = 0
  $h = 0
  if ((-not [int]::TryParse($parts[0], [ref]$w)) -or (-not [int]::TryParse($parts[1], [ref]$h))) { return $null }

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

  if ($RetainAspectValue) { return "-vf scale_cuda=$($targetRes.Width):-1:interp_algo=lanczos" }

  return "-vf scale_cuda=$($targetRes.Width):$($targetRes.Height):interp_algo=lanczos"
}

function Get-AudioMapArg {
  param([string]$AudioLangValue, [object[]]$AudioStreams)

  $audioCount = @($AudioStreams).Count
  if ($AudioLangValue -ieq "nomap" -or $AudioLangValue -ieq "none") { return "" }
  if ($AudioLangValue -ieq "all") {
    if ($audioCount -eq 0) { return "-map 0:a?" }
    return "-map 0:a"
  }

  $matching = @($AudioStreams | Where-Object { $_.tags -and $_.tags.language -ieq $AudioLangValue })
  if ($matching.Count -gt 0) { return "-map 0:a:m:language:$AudioLangValue" }

  if ($audioCount -eq 0) { return "-map 0:a?" }
  return "-map 0:a"
}

function Get-SubMapArg {
  param([string]$SubLangValue, [object[]]$SubStreams)

  $subCount = @($SubStreams).Count
  if ($SubLangValue -ieq "nomap" -or $SubLangValue -ieq "none") { return "" }
  if ($SubLangValue -ieq "all") {
    if ($subCount -eq 0) { return "-map 0:s?" }
    return "-map 0:s"
  }

  $matching = @($SubStreams | Where-Object { $_.tags -and $_.tags.language -ieq $SubLangValue })
  if ($matching.Count -gt 0) { return "-map 0:s:m:language:$SubLangValue" }

  if ($subCount -eq 0) { return "-map 0:s?" }
  return "-map 0:s"
}

function Get-EncodeAnalysis {
  param([object]$Probe)

  $videoStreams = @($Probe.streams | Where-Object { $_.codec_type -ieq 'video' })
  $audioStreams = @($Probe.streams | Where-Object { $_.codec_type -ieq 'audio' })
  $subStreams = @($Probe.streams | Where-Object { $_.codec_type -ieq 'subtitle' })
  $primaryVideo = $videoStreams | Select-Object -First 1

  $audioMap = Get-AudioMapArg -AudioLangValue $AudioLang -AudioStreams $audioStreams
  $subMap = Get-SubMapArg -SubLangValue $SubLang -SubStreams $subStreams
  $scaleArg = Get-ScaleArgumentFromProbe -ResizeValue $ResizeResolution -RetainAspectValue:$RetainAspect -ForceResizeValue:$ForceResize -ForceConvertValue:$ForceConvert -CanScaleUpValue:$CanScaleUp -CanScaleDownValue:$CanScaleDown -PrimaryVideoStream $primaryVideo

  return [PSCustomObject]@{
    AudioMap     = $audioMap
    SubMap       = $subMap
    ScaleArg     = $scaleArg
    PrimaryVideo = $primaryVideo
  }
}

function Invoke-EncodeTestWithFallback {
  param(
    [Parameter(Mandatory = $true)][string]$FFmpegArgs,
    [Parameter(Mandatory = $true)][string]$OutputFile,
    [Parameter(Mandatory = $true)][int]$JobId
  )

  $nullSinkArgs = "-ss 0 -to 10 -f null $($script:NullDevice)"
  $testArgs = $FFmpegArgs.Replace('"' + $OutputFile + '"', $nullSinkArgs)
  $maxRetries = 3
  $retry = 1

  while ($retry -le $maxRetries) {
    Write-VerboseParallelLog -Message "Running ffmpeg test attempt $retry/$maxRetries with args: $testArgs" -JobId $JobId

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
    if (-not [string]::IsNullOrWhiteSpace($err)) {
      $errSummary = ($err -split "`r?`n")[0].Trim()
    }

    Write-ParallelLog -Message "Test command failed (attempt $retry/$maxRetries): $errSummary" -Level Warning -Target Both -JobId $JobId

    if (($retry -eq 1) -and $testArgs.Contains('-hwaccel cuda')) {
      $testArgs = $testArgs.Replace('-hwaccel cuda', '-hwaccel auto')
      Write-ParallelLog -Message "Fallback: switched hwaccel from cuda to auto." -Level Warning -Target Both -JobId $JobId
    }
    elseif (($retry -eq 2) -and $testArgs.Contains('-hwaccel_output_format cuda')) {
      $testArgs = $testArgs.Replace('-hwaccel_output_format cuda', '')
      $testArgs = $testArgs.Replace('scale_cuda=', 'scale=').Replace(':interp_algo=lanczos', ':flags=lanczos')
      Write-ParallelLog -Message "Fallback: removed hwaccel_output_format cuda and switched scaling to CPU filter." -Level Warning -Target Both -JobId $JobId
    }
    elseif (($retry -eq 3) -and $testArgs.Contains('-hwaccel auto')) {
      $testArgs = $testArgs.Replace('-hwaccel auto', '')
      Write-ParallelLog -Message "Fallback: disabled hwaccel as last resort." -Level Warning -Target Both -JobId $JobId
    }

    Start-Sleep -Seconds ([Math]::Pow(2, $retry))
    $retry++
  }

  throw "ffmpeg test command failed after $maxRetries attempts."
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
  param([string]$InputPath)

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

  return @(
    $items |
    Sort-Object $SortExpression |
    Where-Object { $_.Extension -match '^\.(avi|divx|m.*|ts|wmv)' } |
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
    $typeName = [ArrType].GetEnumName($Type)
    Start-ThreadJob -ScriptBlock $scriptBlock -ArgumentList $typeName, $TitleName, $BaseUri, $ApiKey | Out-Null
    Write-ParallelLog -Message "Arr refresh requested." -Target Both -JobId $JobId
  }
  catch {
    Write-ParallelLog -Message "Arr refresh request failed: $($_.Exception.Message)" -Level Warning -Target Both -JobId $JobId
  }
}

function Start-EncodeProcess {
  [CmdletBinding(SupportsShouldProcess = $true)]
  param(
    [int]$JobId,
    [psobject]$JobState
  )

  $item = $JobState.Item
  $probe = $JobState.Probe
  $durationMs = $JobState.DurationMs

  if ($script:CancellationToken.IsCancellationRequested -or $script:StopRequested) {
    $JobState.State = "Skipped"
    $JobState.Status = "Canceled"
    return
  }

  $primaryVideoStream = @($probe.streams | Where-Object { $_.codec_type -ieq 'video' } | Select-Object -First 1)
  if (-not $primaryVideoStream) { throw "No video stream found for '$($item.FullName)'." }

  $analysis = $JobState.Analysis
  $audioMap = $analysis.AudioMap
  $subMap = $analysis.SubMap
  $scaleArg = $analysis.ScaleArg

  Write-VerboseParallelLog -Message "Job analysis selected. AudioMap='$audioMap' SubMap='$subMap' ScaleArg='$scaleArg'" -JobId $JobId

  $bitrateArg = ""
  if (-not [string]::IsNullOrWhiteSpace($BitrateControl)) { $bitrateArg = "-b:v $BitrateControl " }

  $progressTarget = "$($JobState.ProgressFile)"
  if ([string]::IsNullOrWhiteSpace($progressTarget)) { $progressTarget = "pipe:1" }

  $ffmpegArgs = "-hide_banner -y -threads 0 -hwaccel cuda -hwaccel_output_format cuda -copy_unknown -analyzeduration 4GB -probesize 4GB "
  $ffmpegArgs += "-nostats -stats_period 0.25 -progress `"$progressTarget`" -i `"$($item.FullName)`" "
  $ffmpegArgs += "$scaleArg -default_mode infer_no_subs -map 0:v:0 $audioMap $subMap -dn -map_metadata 0 -map_chapters 0 "
  $ffmpegArgs += "-c:v hevc_nvenc -c:a copy -c:s copy "
  $ffmpegArgs += "$bitrateArg-bufsize:v 5MB -preset:v p4 -tune:v hq -tier:v main -rc:v constqp "
  $ffmpegArgs += "-init_qpI:v $($script:CQPRateControlInt) -init_qpP:v $($script:CQPRateControlInt + 1) -init_qpB:v $($script:CQPRateControlInt + 2) "
  $ffmpegArgs += "-rc-lookahead:v 32 -spatial-aq:v 1 -aq-strength:v 8 -temporal-aq:v 1 -b_ref_mode:v 1 "
  $ffmpegArgs += "-max_interleave_delta 500000 "
  $ffmpegArgs += "`"$($JobState.OutputFile)`""

  $ffmpegArgs = Invoke-EncodeTestWithFallback -FFmpegArgs $ffmpegArgs -OutputFile $JobState.OutputFile -JobId $JobId
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

  $JobState.Process = $p
  $JobState.Started = Get-Date
  $JobState.DurationMs = $durationMs
  $JobState.State = "Running"
  $JobState.ProgressReadTask = $p.StandardOutput.ReadLineAsync()
  $JobState.StderrTask = $p.StandardError.ReadToEndAsync()

  # Open a per-job progress dump file for raw postmortem analysis.
  if ($LogVerbose) {
    $dumpName = "ffmpeg_nvenc_h265_progress_job${JobId}_$(Get-Date -Format 'yyyyMMddHHmmss').log"
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
  $running = @($StateTable.Values | Where-Object { $_.State -eq "Running" }).Count
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

      if ($RefreshArrOnCompletion) {
        $JobState.Status = "Refreshing Arr..."
        $arr = Get-ArrContext -SourceFile $srcFile
        if (-not $arr) {
          Write-ParallelLog -Message "Skipping Arr refresh: source path '$($srcFile.FullName)' is outside configured movies_path '$movies_path' and tv_shows_path '$tv_shows_path'." -Level Warning -Target Both -JobId $JobId
        }
        else {
          $titleName = (($arr.Type -eq [ArrType]::series -or $srcFile.Directory.Name.ToLower().StartsWith("season")) ? $srcFile.Directory.Parent.Name : $srcFile.Directory.Name)
          Invoke-ArrRefresh -Type $arr.Type -TitleName $titleName -BaseUri $arr.BaseUri -ApiKey $arr.ApiKey -JobId $JobId
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

    if ($RefreshArrOnCompletion) {
      $JobState.Status = "Refreshing Arr..."
      $arr = Get-ArrContext -SourceFile $srcFile
      if (-not $arr) {
        Write-ParallelLog -Message "Skipping Arr refresh: source path '$($srcFile.FullName)' is outside configured movies_path '$movies_path' and tv_shows_path '$tv_shows_path'." -Level Warning -Target Both -JobId $JobId
      }
      else {
        $titleName = (($arr.Type -eq [ArrType]::series -or $srcFile.Directory.Name.ToLower().StartsWith("season")) ? $srcFile.Directory.Parent.Name : $srcFile.Directory.Name)
        Invoke-ArrRefresh -Type $arr.Type -TitleName $titleName -BaseUri $arr.BaseUri -ApiKey $arr.ApiKey -JobId $JobId
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
      [void]$PendingQueue.Dequeue()
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
    # Running processes are terminated best-effort; final state is reconciled in the host loop.
    foreach ($active in @($StateTable.Values | Where-Object { $_.State -eq 'Running' })) {
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
        foreach ($active in @($script:StateTableRef.Values | Where-Object { $_ -and $_.Process -and ($_.State -eq 'Running') })) {
          if (-not $active.Process.HasExited) {
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
        }
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
Register-CancellationHandler

# Clean up old log files (main logs and progress dumps).
$logFilter = Join-Path $script:log_path "ffmpeg_nvenc_h265_*.log"
$progressFilter = Join-Path $script:log_path "ffmpeg_nvenc_h265_progress_job*.log"
$oldLogs = @(Get-Item -Path $logFilter, $progressFilter -ErrorAction SilentlyContinue | Where-Object {
    $_.FullName -ne $script:logFilePath -and $_.CreationTime -lt (Get-Date).AddMinutes(-60)
  })
if ($oldLogs.Count -gt 0) {
  Write-ParallelLog -Message "Cleaning up $($oldLogs.Count) old log file(s)..." -Target Both
  $removedCount = 0
  foreach ($log in $oldLogs) {
    $logSplit = ($log.BaseName -split "_")
    if ($logSplit.Count -ge 4) {
      $today = (Get-Date).Date
      $dateStr = ($logSplit[3] -replace '[^0-9]', '')
      if ($dateStr.Length -ge 8) {
        $logday = [DateTime]::ParseExact($dateStr.Substring(0, 8), "yyyyMMdd", $null)
        if (($logday -lt $today.AddDays(-1)) -or ($log.Length -lt 2KB)) {
          Remove-Item -LiteralPath $log.FullName -Force -ErrorAction SilentlyContinue
          $removedCount++
        }
      }
    }
  }
  if ($removedCount -gt 0) {
    Write-ParallelLog -Message "Removed $removedCount old log file(s)." -Target Both
  }
}

try {
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

  while (($candidates.Count -gt 0) -or ($pending.Count -gt 0) -or (@($sync.Values | Where-Object { $_.State -eq 'Running' }).Count -gt 0)) {
    if ($script:CancellationToken.IsCancellationRequested -and (-not $script:StopRequested)) {
      $canceledPendingCount += Stop-ParallelExecution -StateTable $sync -PendingQueue $pending -CandidateQueue $candidates -Reason "Cancellation requested"
    }

    if ($script:StopRequested) { break }

    # Only triage when we need to refill capacity. Avoid ffprobe blocking while all encoders are busy.
    $runningBeforeTriage = @($sync.Values | Where-Object { $_.State -eq 'Running' }).Count
    $needRefill = (($runningBeforeTriage + $pending.Count) -lt $MaxParallelJobs)
    $triageBudget = if ($runningBeforeTriage -gt 0) { 2 }
    else { [Math]::Max(6, ($MaxParallelJobs * 3)) }

    $triagedThisTick = 0

    while ($needRefill -and ($pending.Count -lt $MaxParallelJobs) -and ($candidates.Count -gt 0) -and ($triagedThisTick -lt $triageBudget)) {
      if ($script:StopRequested -or $script:CancellationToken.IsCancellationRequested) { break }

      $candidate = $candidates.Dequeue()
      $work = Get-QueuedWorkItem -Item $candidate -JobId $nextId
      if ($work) {
        $pending.Enqueue($work)
        $nextId++
      }

      $triagedThisTick++
      $needRefill = (($runningBeforeTriage + $pending.Count) -lt $MaxParallelJobs)
    }

    # Admission control: never exceed MaxParallelJobs active encodes.
    while ((@($sync.Values | Where-Object { $_.State -eq 'Running' }).Count -lt $MaxParallelJobs) -and ($pending.Count -gt 0)) {
      if ($script:StopRequested -or $script:CancellationToken.IsCancellationRequested) { break }

      $work = $pending.Dequeue()
      $sync[$work.Id] = $work
      Start-EncodeProcess -JobId $work.Id -JobState $sync[$work.Id]
    }

    Update-ProgressState -StateTable $sync
    Show-ParallelProgress -StateTable $sync -TotalCount $totalKnown

    if ($LogVerbose) {
      $now = Get-Date
      if (($now - $script:LastVerboseSnapshot).TotalSeconds -ge 5.0) {
        $script:LastVerboseSnapshot = $now
        $runningCount = @($sync.Values | Where-Object { $_.State -eq 'Running' }).Count
        $completedCount = @($sync.Values | Where-Object { $_.State -in @('Completed', 'Failed', 'Skipped') }).Count
        $pendingCount = $pending.Count + $candidates.Count
        $jobProgressParts = @()
        foreach ($jk in @($sync.Keys | Sort-Object)) {
          $jv = $sync[$jk]
          if ($jv -and ($jv.State -eq 'Running')) {
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
    foreach ($active in @($sync.Values | Where-Object { $_ -and $_.Process -and ($_.State -eq 'Running') })) {
      if (-not $active.Process.HasExited) {
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

  if ($script:LogMutex) {
    try { $script:LogMutex.Dispose() }
    catch {
      if ($LogVerbose) {
        Write-Verbose "Unable to dispose log mutex cleanly: $($_.Exception.Message)"
      }
    }
    $script:LogMutex = $null
  }

  $script:StateTableRef = $null
  $script:PendingQueueRef = $null
  $script:CandidateQueueRef = $null
  $script:TotalWorkCount = 0
  Unregister-CancellationHandler
}