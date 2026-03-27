$ErrorActionPreference = 'Stop'

$baselineFfmpeg = @(Get-Process -Name ffmpeg -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Id)
$scriptPath = 'C:\Users\Administrator\OneDrive\Documents\ffmpeg_convert_nvenc_parallel.ps1'
$commandText = "& '$scriptPath' -Path 'V:\Media\TV Shows\' -MaxParallelJobs 3 -SortExpression @{ e='Name'; Descending=`$true } -UserFilter { `$_.Name -match '(?:h|x)264|av1|avc|mpeg-?2|vc-?1|xvid|divx|vp9' } -ForceConvert -LogEnabled -LogVerbose"

$hostProcess = Start-Process -FilePath 'pwsh' -ArgumentList @('-NoProfile','-ExecutionPolicy','Bypass','-Command',$commandText) -PassThru

$newFfmpeg = @()
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
while ($stopwatch.Elapsed.TotalSeconds -lt 120) {
    $currentFfmpeg = @(Get-Process -Name ffmpeg -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Id)
    $detected = @($currentFfmpeg | Where-Object { $_ -notin $baselineFfmpeg } | Sort-Object -Unique)
    if ($detected.Count -gt 0) {
        $newFfmpeg = $detected
        break
    }
    Start-Sleep -Seconds 2
}

Stop-Process -Id $hostProcess.Id -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 4

$remaining = @()
if ($newFfmpeg.Count -gt 0) {
    $remaining = @(Get-Process -Name ffmpeg -ErrorAction SilentlyContinue | Where-Object { $_.Id -in $newFfmpeg } | Select-Object -ExpandProperty Id | Sort-Object -Unique)
}

# cleanup leftover ffmpeg from this run
if ($newFfmpeg.Count -gt 0) {
    @(Get-Process -Name ffmpeg -ErrorAction SilentlyContinue | Where-Object { $_.Id -in $newFfmpeg }) | Stop-Process -Force -ErrorAction SilentlyContinue
}

$result = [PSCustomObject]@{
    HostProcessId = $hostProcess.Id
    NewFfmpegPidsDetected = if ($newFfmpeg.Count -gt 0) { $newFfmpeg } else { @() }
    RemainingFfmpegPidsAfterHostKill = if ($remaining.Count -gt 0) { $remaining } else { @() }
    Result = if ($remaining.Count -eq 0) { 'PASS' } else { 'FAIL' }
}

$result | ConvertTo-Json -Depth 4
