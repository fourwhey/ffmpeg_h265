#Requires -Version 7.0
<#
.SYNOPSIS
  Starts a local HTTP server and opens the MediaAudit report in the browser.
.DESCRIPTION
  Starts a System.Net.HttpListener on the specified port, serves files from this
  script directory, and opens either mediaaudit_report_sample.html or
  mediaaudit_report.html from that served root in the default browser.
  Requests for .db files are served as NDJSON by querying the SQLite database
  via PSSQLite (must be installed).
.NOTES
  - Base URL: http://localhost:<Port>/
  - Default page: mediaaudit_report_sample.html
  - Press Ctrl+C to stop the server.
#>
param(
  [int] $Port = 8080,
  [ValidateSet('sample', 'runtime')][string] $Report = 'sample'
)

$root = $PSScriptRoot
if (-not $root) { $root = $PWD.Path }

$defaultPage = if ($Report -eq 'sample' -and (Test-Path -LiteralPath (Join-Path $root 'mediaaudit_report_sample.html') -PathType Leaf)) {
  'mediaaudit_report_sample.html'
} else {
  'mediaaudit_report.html'
}

$baseUrl   = "http://localhost:$Port/"
$reportUrl = "${baseUrl}$defaultPage"

$listener = [System.Net.HttpListener]::new()
$listener.Prefixes.Add($baseUrl)

try {
  $listener.Start()
} catch {
  Write-Error "Failed to start HTTP listener on port ${Port}: $_"
  exit 1
}

Write-Host "Serving  $root" -ForegroundColor Cyan
Write-Host "Opening  $reportUrl" -ForegroundColor Cyan
Write-Host "Press Ctrl+C to stop.`n" -ForegroundColor Yellow

Start-Process $reportUrl

$mimeTypes = @{
  '.html'   = 'text/html; charset=utf-8'
  '.ndjson' = 'application/x-ndjson; charset=utf-8'
  '.json'   = 'application/json; charset=utf-8'
  '.js'     = 'application/javascript; charset=utf-8'
  '.css'    = 'text/css; charset=utf-8'
  '.ico'    = 'image/x-icon'
}

# Query a SQLite MediaAudit database and return all rows as an NDJSON string.
function Get-DbAsNdjson {
  param([string]$DbPath)

  Import-Module PSSQLite -ErrorAction Stop

  # 4 bulk queries instead of N+1 per row
  $rows     = Invoke-SqliteQuery -DataSource $DbPath -Query 'SELECT * FROM mediaaudit ORDER BY path'
  $allAudio = Invoke-SqliteQuery -DataSource $DbPath -Query 'SELECT mediaaudit_id, language, codec, channels FROM audio_streams ORDER BY mediaaudit_id, rowid'
  $allSubs  = Invoke-SqliteQuery -DataSource $DbPath -Query 'SELECT mediaaudit_id, language FROM subtitle_streams ORDER BY mediaaudit_id, rowid'
  $allTags  = Invoke-SqliteQuery -DataSource $DbPath -Query 'SELECT mediaaudit_id, tag FROM tags ORDER BY mediaaudit_id, rowid'

  # Index related rows by mediaaudit_id
  $audioMap = @{}
  foreach ($a in $allAudio) {
    if (-not $audioMap.ContainsKey($a.mediaaudit_id)) { $audioMap[$a.mediaaudit_id] = [System.Collections.Generic.List[object]]::new() }
    $audioMap[$a.mediaaudit_id].Add($a)
  }
  $subMap = @{}
  foreach ($s in $allSubs) {
    if (-not $subMap.ContainsKey($s.mediaaudit_id)) { $subMap[$s.mediaaudit_id] = [System.Collections.Generic.List[object]]::new() }
    $subMap[$s.mediaaudit_id].Add($s)
  }
  $tagMap = @{}
  foreach ($t in $allTags) {
    if (-not $tagMap.ContainsKey($t.mediaaudit_id)) { $tagMap[$t.mediaaudit_id] = [System.Collections.Generic.List[object]]::new() }
    $tagMap[$t.mediaaudit_id].Add($t)
  }

  $sb = [System.Text.StringBuilder]::new()

  foreach ($row in $rows) {
    $audio     = $audioMap[$row.id]
    $subtitles = $subMap[$row.id]
    $tags      = $tagMap[$row.id]

    $meta = [ordered]@{
      path      = $row.path
      size      = $row.size
      mtime     = $row.mtime
      ctime     = $row.ctime
      hash      = $row.hash
      duration  = $row.duration_ms
      library   = $row.library
      video     = [ordered]@{
        codec   = $row.video_codec
        bitrate = $row.video_bitrate
        width   = $row.video_width
        height  = $row.video_height
        hdr     = [bool]$row.video_hdr
      }
      audio     = @(if ($audio)     { $audio     | ForEach-Object { [ordered]@{ lang = $_.language; codec = $_.codec; channels = $_.channels } } })
      subtitles = @(if ($subtitles) { $subtitles | ForEach-Object { $_.language } })
      tags      = @(if ($tags)      { $tags      | ForEach-Object { $_.tag } })
    }

    $null = $sb.AppendLine(($meta | ConvertTo-Json -Compress -Depth 10))
  }

  return $sb.ToString()
}

try {
  while ($listener.IsListening) {
    # Poll GetContextAsync so Ctrl+C can interrupt without event-callback runspace issues.
    $contextTask = $listener.GetContextAsync()
    while (-not $contextTask.IsCompleted) {
      Start-Sleep -Milliseconds 100
    }
    if ($contextTask.IsFaulted) { break }

    $ctx  = $contextTask.Result
    $req  = $ctx.Request
    $resp = $ctx.Response

    $reqPath = $req.Url.LocalPath.TrimStart('/')
    if ([string]::IsNullOrEmpty($reqPath)) { $reqPath = $defaultPage }

    # Resolve and guard against path traversal.
    $resolvedRoot = [System.IO.Path]::GetFullPath($root)
    $resolvedFile = [System.IO.Path]::GetFullPath((Join-Path $root $reqPath))

    $underRoot = $resolvedFile.StartsWith(
      $resolvedRoot + [System.IO.Path]::DirectorySeparatorChar,
      [System.StringComparison]::OrdinalIgnoreCase
    )

    if (-not $underRoot) {
      $resp.StatusCode = 403
      $resp.Close()
      Write-Host "$(Get-Date -Format 'HH:mm:ss')  403  $($req.Url.LocalPath)" -ForegroundColor Red
      continue
    }

    if (-not (Test-Path -LiteralPath $resolvedFile -PathType Leaf)) {
      $resp.StatusCode = 404
      $resp.Close()
      Write-Host "$(Get-Date -Format 'HH:mm:ss')  404  $($req.Url.LocalPath)" -ForegroundColor DarkYellow
      continue
    }

    $ext  = [System.IO.Path]::GetExtension($resolvedFile).ToLower()

    # Serve .db files as NDJSON by querying SQLite.
    if ($ext -eq '.db') {
      try {
        $ndjson = Get-DbAsNdjson -DbPath $resolvedFile
        $bytes  = [System.Text.Encoding]::UTF8.GetBytes($ndjson)
        $resp.ContentType     = 'application/x-ndjson; charset=utf-8'
        $resp.ContentLength64 = $bytes.Length
        $resp.OutputStream.Write($bytes, 0, $bytes.Length)
        Write-Host "$(Get-Date -Format 'HH:mm:ss')  200  $($req.Url.LocalPath)  (db→ndjson)"
      } catch {
        $resp.StatusCode = 500
        Write-Host "$(Get-Date -Format 'HH:mm:ss')  500  $($req.Url.LocalPath)  $_" -ForegroundColor Red
      } finally {
        $resp.Close()
      }
      continue
    }

    $mime = if ($mimeTypes.ContainsKey($ext)) { $mimeTypes[$ext] } else { 'application/octet-stream' }

    try {
      $bytes = [System.IO.File]::ReadAllBytes($resolvedFile)
      $resp.ContentType     = $mime
      $resp.ContentLength64 = $bytes.Length
      $resp.OutputStream.Write($bytes, 0, $bytes.Length)
      Write-Host "$(Get-Date -Format 'HH:mm:ss')  200  $($req.Url.LocalPath)"
    } catch {
      $resp.StatusCode = 500
      Write-Host "$(Get-Date -Format 'HH:mm:ss')  500  $($req.Url.LocalPath)  $_" -ForegroundColor Red
    } finally {
      $resp.Close()
    }
  }
} finally {
  $listener.Stop()
  $listener.Close()
  Write-Host "`nServer stopped." -ForegroundColor Yellow
}
