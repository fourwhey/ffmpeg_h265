#Requires -Version 7.0
<#
.SYNOPSIS
  Starts a local HTTP server and opens the metadata report in the browser.
.DESCRIPTION
  Starts a System.Net.HttpListener on the specified port, serves files from this
  script directory, and opens metadata_report.html from that served root in the
  default browser.
.NOTES
  - Base URL: http://localhost:<Port>/
  - Default page: metadata_report.html
  - Press Ctrl+C to stop the server.
#>
param(
  [int] $Port = 8080
)

$root = $PSScriptRoot
if (-not $root) { $root = $PWD.Path }

$baseUrl   = "http://localhost:$Port/"
$reportUrl = "${baseUrl}metadata_report.html"

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
    if ([string]::IsNullOrEmpty($reqPath)) { $reqPath = 'metadata_report.html' }

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
