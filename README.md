# FFmpeg NVENC H.265 Parallel Encoder

A PowerShell script for high-performance, parallel video transcoding using FFmpeg hardware acceleration (NVENC, AMF, QSV) with software fallback. This script is designed to efficiently convert large video libraries to H.265 (HEVC) format with support for concurrent processing, progress tracking, and media server integration.

## Purpose

This script automates the batch conversion of video files to H.265 format using available GPU hardware encoding. It's particularly useful for:

- **Media Library Optimization**: Reduce storage requirements by converting existing video collections to efficient H.265 encoding
- **Automated Processing**: Integrate with media management systems (Radarr/Sonarr) for seamless library maintenance
- **High-Performance Encoding**: Leverage NVIDIA NVENC, AMD AMF, or Intel QSV for fast, parallel video processing
- **Quality Control**: Maintain video quality while reducing file sizes through configurable quality settings

## Key Features

- **Parallel Processing**: Run up to 4 concurrent encoding jobs (default: 3)
- **Real-time Progress Tracking**: Per-job progress bars showing encoding speed, completion percentage, and ETA
- **Smart Video Analysis**: Automatic detection of video properties (resolution, codec, audio/subtitle streams)
- **Flexible Resizing**: Scale videos to specific resolutions with aspect ratio preservation
- **Media Server Integration**: Automatic refresh of Radarr (movies) and Sonarr (TV shows) after processing
- **Thread-safe Logging**: Detailed logging with job-specific tracking and verbose mode
- **Graceful Cancellation**: Ctrl+C handling with proper cleanup of running processes
- **Incremental Processing**: Skip already-converted files and resume interrupted batches
- **Metadata Analysis Mode**: Scan video libraries and generate comprehensive metadata reports with HTML visualization

## Metadata Analysis Mode

The script supports a metadata analysis mode (`-Analyze`) that scans video files, extracts detailed metadata, and generates reports without performing any encoding.

### Features

- **Comprehensive Metadata**: Collects file hashes, timestamps, video/audio/subtitle details, and FFprobe data
- **NDJSON Output**: Appends metadata to a newline-delimited JSON file for efficient processing
- **Index Sidecar**: Maintains a fast-lookup index with file offsets and timestamps
- **HTML Reports**: Generates an interactive dark-theme dashboard with top-level filters, metric cards, and searchable metadata tables
- **Automatic Compaction**: Removes entries for deleted files and maintains current state
- **Parallel Run Prevention**: Uses mutex to prevent concurrent analysis runs on the same directory
- **Hash Strategies**: Supports `size-mtime` change tracking (Analyze default, fastest), plus MD5 and SHA256 content hashing

### Usage

```powershell
# Basic analysis (default uses size+mtime change tracking for speed)
.\ffmpeg_h265.ps1 -Path "Z:\Movies" -Analyze

# Force MD5 content hashing
.\ffmpeg_h265.ps1 -Path "Z:\Movies" -Analyze -HashAlgorithm md5

# Use SHA256 for stronger hashing
.\ffmpeg_h265.ps1 -Path "Z:\Movies" -Analyze -HashAlgorithm sha256

# Compact existing metadata (remove deleted files)
.\ffmpeg_h265.ps1 -Path "Z:\Movies" -Compact
```

### Output Files

Analysis creates the following files in the resolved log/output directory (`log_path` / `FFENC_LOG_PATH`):

- `metadata.ndjson`: Newline-delimited JSON with one metadata object per line
- `metadata_index.json`: Index array for fast lookup by path and timestamp
- `metadata_report.html`: HTML table visualization of library metadata

### Repository Samples

The repository includes sample analysis artifacts that are intended to work standalone when opened locally:

- `metadata.ndjson`: sample NDJSON metadata data
- `metadata_index.json`: sample index sidecar
- `metadata_report.html`: self-contained sample dark-theme report generated from sample NDJSON
- The HTML report template is embedded in the script and generated at runtime (there is no standalone template file in the repo)

These sample files are examples only. Runtime Analyze output is still written to your configured output directory (`log_path` / `FFENC_LOG_PATH`).

### Metadata Schema

Each NDJSON entry contains:

```json
{
  "path": "Z:/Movies/Big Buck Bunny (2008)/Big.Buck.Bunny.2008.mkv",
  "size": 734003200,
  "mtime": "2026-03-16T12:00:00Z",
  "ctime": "2026-03-16T12:00:00Z",
  "hash": "size-mtime:734003200:638777184000000000",
  "duration": 596,
  "video": {
    "codec": "h264",
    "bitrate": 8500000,
    "width": 1920,
    "height": 1080,
    "hdr": false
  },
  "audio": [
    { "lang": "eng", "codec": "ogg", "channels": 2 }
  ],
  "subtitles": ["eng"],
  "tags": ["movie", "drama"]
}
```

## Requirements

- **PowerShell 7.5 or newer**
- **FFmpeg/FFprobe**: Installed and reachable via `ffmpeg_path` (directory or full ffmpeg executable path)
- **Hardware Encoder (optional, auto-detected)**: NVIDIA NVENC, AMD AMF, or Intel QSV
- **OS**: Windows, Linux, or macOS with PowerShell 7.5+

## Configuration

There are no built-in defaults for media paths, FFmpeg path, or Arr settings.
Each required value is resolved in this order: matching environment variable, then config file value, then fail.

### Configuration Surface Policy

To keep behavior predictable and avoid drift, the script uses two distinct surfaces:

- **Command parameters**: per-run behavior (what to scan, concurrency, filters, resize, logging switches)
- **Environment/config values**: environment/deployment settings (paths, endpoints, API keys)

For environment/deployment settings, precedence is always:

1. Environment variable
2. Config file value
3. Fail fast for required settings

For `log_path`, step 3 is different: if missing from env/config, the script falls back to config discovery locations (loaded config directory, then current directory, then script directory).

`-ConfigPath` is the only configuration-related parameter and only controls where config is loaded from.

### Config File (Optional Fallback)

Create a `ffmpeg_h265.config.json` file with your settings. The script searches for this file in:

1. **Explicit path** - Use `-ConfigPath` parameter to specify location
2. **Current directory** - Where you run the script from
3. **Script directory** - Where the `.ps1` file is located

**Example ffmpeg_h265.config.json:**

```json
{
  "ffmpeg_path": "C:\\ffmpeg",
  "log_path": "V:\\Logs\\ffmpeg_h265",
  "processed_path": "V:\\Processed",
  "processing_path": "V:\\ProcessingTemp",
  "media_path": "V:\\Media",
  "movies_subfolder": "Movies",
  "tv_shows_subfolder": "TV Shows",
  "radarr_baseUri": "http://192.168.10.15:7878",
  "radarr_apiKey": "your-radarr-api-key-here",
  "sonarr_baseUri": "http://192.168.10.15:8989",
  "sonarr_apiKey": "your-sonarr-api-key-here",
  "arr_refresh_timeout_seconds": 60
}
```

`movies_subfolder` and `tv_shows_subfolder` are optional. They default to `Movies` and `TV Shows`.
Use them when your library uses different names, for example `films` and `series`.

`arr_refresh_timeout_seconds` controls how long the script waits for Radarr/Sonarr refresh operations to complete (default: 15 seconds). Increase this value if you have large libraries that take longer to refresh.

**Setup:**

Config file setup is optional. The default/primary configuration source is environment variables.

1. (Optional) Copy `ffmpeg_h265.config.example.json` to `ffmpeg_h265.config.json` in your folder
2. (Optional) Edit the file with your actual API keys and paths
3. Set environment variables for primary config values (see mapping below; `log_path` is optional)
4. Run the script normally; if any value is missing from env vars, the script falls back to config file discovery

### Environment Variable Fallback (No Defaults)

Environment variables are checked first for every required value.
You can keep values in config as fallback where env vars are not set.
If both are missing/empty, the script fails fast (except `log_path`, which has discovery-based fallback).

Mapping:

- `ffmpeg_path` → `FFENC_FFMPEG_PATH`
- `log_path` (optional) → `FFENC_LOG_PATH`
- `processed_path` → `FFENC_PROCESSED_PATH`
- `processing_path` → `FFENC_PROCESSING_PATH`
- `media_path` → `FFENC_MEDIA_PATH`
- `movies_subfolder` (optional) → `FFENC_MOVIES_SUBFOLDER`
- `tv_shows_subfolder` (optional) → `FFENC_TV_SHOWS_SUBFOLDER`
- `radarr_baseUri` → `RADARR_BASE_URI`
- `radarr_apiKey` → `RADARR_API_KEY`
- `sonarr_baseUri` → `SONARR_BASE_URI`
- `sonarr_apiKey` → `SONARR_API_KEY`
- `arr_refresh_timeout_seconds` (optional) → `FFENC_ARR_REFRESH_TIMEOUT_SECONDS`
- `-ConfigPath` parameter (optional) → `FFENC_CONFIGPATH`

### ConfigPath Parameter

Explicitly specify where to load ffmpeg_h265.config.json from:

```powershell
.\ffmpeg_h265.ps1 -Path "V:\Videos" -ConfigPath "D:\Configs"
```

This overrides automatic discovery and only looks in the specified directory.

Alternatively, set the `FFENC_CONFIGPATH` environment variable to avoid passing the parameter each time:

```powershell
$env:FFENC_CONFIGPATH = "D:\Configs"
.\ffmpeg_h265.ps1 -Path "V:\Videos"
```

The `-ConfigPath` parameter takes precedence over the environment variable if both are set.

## Usage

### Basic Usage

Convert all video files in a directory using default settings. The `-Path` parameter accepts the full path to your media directory:

```powershell
.\ffmpeg_h265.ps1 -Path "V:\Media\TV Shows"
```

The script automatically detects whether to refresh Radarr (Movies) or Sonarr (TV Shows) based on the path structure.

### Common Examples

#### Example 1: Convert with Custom Quality and Parallel Jobs

```powershell
.\ffmpeg_h265.ps1 -Path "V:\Media\Movies" -MaxParallelJobs 4 -CQPRateControl 26
```

Converts movies with 4 parallel jobs and CQP quality level 26 (lower = higher quality).

#### Example 2: Resize Videos to 1080p

```powershell
.\ffmpeg_h265.ps1 -Path "V:\Videos" -ResizeResolution "1920:1080" -RetainAspect -CanScaleDown
```

Resizes videos to 1080p, maintaining aspect ratio, only if the source is larger than 1080p.

#### Example 3: Force Conversion with Bitrate Control

```powershell
.\ffmpeg_h265.ps1 -Path "V:\Media\TV Shows" -BitrateControl "6M" -ForceConvert -MaxParallelJobs 2
```

Forces re-encoding using 6 Mbps bitrate limit, even if files are already H.265 encoded.

#### Example 4: Enable Logging and Verbose Output

```powershell
.\ffmpeg_h265.ps1 -Path "V:\Videos" -LogEnabled -LogVerbose -ShowOutputCmd
```

Enables detailed logging with verbose output and displays FFmpeg commands being executed.

#### Example 5: Process Only Recent Files

```powershell
.\ffmpeg_h265.ps1 -Path "V:\Media\Movies" -LastRunDate (Get-Date).AddDays(-7)
```

Only processes files modified in the last 7 days.

#### Example 6: Custom Filtering and Sorting

```powershell
.\ffmpeg_h265.ps1 -Path "V:\Videos" `
    -UserFilter { $_.Length -gt 500MB } `
    -SortExpression @{ e = 'Length'; Descending = $true }
```

Processes only files larger than 500MB, starting with the largest files first.

#### Example 7: Skip Post-Processing

```powershell
.\ffmpeg_h265.ps1 -Path "V:\Videos" -SkipMoveOnCompletion -SkipArrRefresh
```

Converts videos but skips moving files to the processed folder and skips Radarr/Sonarr refresh.

#### Example 8: Quality Comparison with Resize

```powershell
.\ffmpeg_h265.ps1 -Path "V:\Test" `
    -ResizeResolution "1280:720" `
    -ForceResize `
    -RetainAspect `
    -CQPRateControl 28 `
    -MaxParallelJobs 1 `
    -ShowOutputCmd
```

Single-threaded conversion with forced 720p resize for quality testing, showing the exact FFmpeg commands.

#### Example 9: Using ConfigPath for Explicit Config Location

```powershell
.\ffmpeg_h265.ps1 -Path "V:\Media\TV Shows" `
    -ConfigPath "D:\Configs"
```

Explicitly specify config file location (overrides auto-discovery search). The script loads `ffmpeg_h265.config.json` from the specified directory.

#### Example 10: ConfigPath with Environment Variables

```powershell
$env:FFMPEG_PATH = "V:\ffmpeg"
$env:FFMPEG_LOG_PATH = "V:\Logs"
$env:FFMPEG_PROCESSED_PATH = "V:\Processed"
$env:FFMPEG_PROCESSING_PATH = "V:\ProcessingTemp"
$env:FFMPEG_MEDIA_PATH = "V:\Media"

.\ffmpeg_h265.ps1 -Path "V:\Media\Movies" -ConfigPath "D:\Configs" -MaxParallelJobs 4
```

Use environment variables as primary config (checked first), with ConfigPath as fallback for any config file values. This is ideal for containerized/cloud deployments where env vars are preferred.

## Parameters

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `-Path` | string | Input path containing video files to convert |

### Optional Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `-MaxParallelJobs` | int | 3 | Maximum number of concurrent encoding jobs (1-4). Values above 4 are clamped to 4. |
| `-ShowOutputCmd` | switch | false | Display FFmpeg commands being executed |
| `-NoProgress` | switch | false | Disable progress bars |
| `-ResizeResolution` | string | "" | Target resolution (e.g., "1920:1080") |
| `-ForceResize` | switch | false | Force resize even if already at target resolution |
| `-RetainAspect` | switch | false | Maintain aspect ratio when resizing |
| `-CanScaleUp` | switch | false | Allow upscaling videos |
| `-CanScaleDown` | switch | false | Allow downscaling videos |
| `-CQPRateControl` | string | "28" | Constant Quality (CQP) value (lower = higher quality, 18-32 typical) |
| `-BitrateControl` | string | "" | Target bitrate (e.g., "6M" for 6 Mbps) instead of CQP |
| `-ForceConvert` | switch | false | Re-encode files already in H.265 format |
| `-CanReprocess` | switch | false | Allow reprocessing of previously completed files |
| `-SkipMoveOnCompletion` | switch | false | Don't move files to processed folder after conversion |
| `-SkipArrRefresh` | switch | false | Skip Radarr/Sonarr refresh after completion |
| `-LogEnabled` | switch | false | Enable file logging |
| `-LogVerbose` | switch | false | Enable verbose logging |
| `-ExitOnError` | switch | false | Stop all processing if any job fails |
| `-SortExpression` | hashtable | Name, Ascending | Sort order for processing files |
| `-UserFilter` | scriptblock | All files | Custom filter for selecting files |
| `-AudioLang` | string[] | @("eng","jpn") | ISO 639-2 audio language codes to keep; supports one or many values |
| `-SubLang` | string[] | @("eng") | ISO 639-2 subtitle language codes to keep; supports one or many values |
| `-LastRunDate` | datetime | - | Only process files modified after this date |
| `-SkipFileLock` | switch | false | Skip file lock check (process files even if in use) |
| `-ConfigPath` | string | "" | Directory containing ffmpeg_h265.config.json (overrides auto-discovery) |
| `-Encoder` | string | "auto" | Encoder profile: `auto`, `nvenc`, `amf`, `qsv`, `software` |

When `-Encoder auto` is used (default), the script detects available HEVC encoders from ffmpeg and picks one automatically. Set `-Encoder` to a specific value to bypass detection.

### Audio and Subtitle Language Behavior

Both `-AudioLang` and `-SubLang` accept one or more [ISO 639-2](https://en.wikipedia.org/wiki/List_of_ISO_639-2_codes) three-letter language codes. Current defaults: `-AudioLang @("eng","jpn")` and `-SubLang @("eng")`.

String and array forms are both acceptable for `-AudioLang` and `-SubLang`.
Examples: `-AudioLang eng`, `-AudioLang @("eng","jpn")`, `-SubLang eng`, `-SubLang @("eng")`.

Special values are supported in either singular or array form.

| Value | Outcome |
|---|---|
| `eng` (or any single language code) | Only streams tagged with that language are kept. If no matching streams exist in the file, **all** streams of that type are kept as a fallback. |
| `@("eng","jpn")` | Streams matching any listed language are kept. Languages are applied in the order provided. If no matching streams exist, **all** streams of that type are kept as a fallback. |
| `all` | All streams of that type are kept regardless of language tag. |
| `nomap` or `none` | No streams of that type are mapped — the track is completely excluded from the output. |

If the source file contains no audio or subtitle streams at all, the map is made optional so ffmpeg does not error out.

### Default Stream Selection

When audio/subtitle streams are mapped, the script explicitly sets a default output audio stream and default output subtitle stream via ffmpeg disposition flags.

- Default is selected from the mapped output streams (in mapped order)
- The script prefers non-commentary/non-descriptive tracks for default when possible
- If all mapped tracks look like commentary/descriptive variants, it still sets a deterministic default
- If no streams of that type are mapped, no default disposition is set for that type

## Quality Settings Guide

### CQP (Constant Quality) Values

- **18-22**: Very high quality, larger files (nearly lossless)
- **23-26**: High quality, good balance (recommended for archival)
- **27-28**: Good quality, smaller files (recommended default)
- **29-32**: Acceptable quality, significant compression

### Bitrate Recommendations

- **4K (2160p)**: 20-35 Mbps
- **1080p**: 6-12 Mbps
- **720p**: 3-6 Mbps
- **480p**: 1-3 Mbps

## Output Organization

The script uses configurable paths for file organization (customize via ffmpeg_h265.config.json):

**Example paths (in config):**
- **Processing Temp**: `V:\ProcessingTemp`
- **Processed**: `V:\Processed`
- **Media Root**: `V:\Media`
  - TV Shows: `V:\Media\TV Shows`
  - Movies: `V:\Media\Movies`

See the **Configuration** section above for how to customize these paths.

## Logging

When `-LogEnabled` is specified, logs are created in the configured log directory:

Logs are written to `log_path` from config (or `FFMPEG_LOG_PATH` from environment variables).
If `log_path` is not provided, the script uses config discovery locations (loaded config directory, then current directory, Documents, script directory).

- **Main Log Format**: `ffmpeg_h265_YYYYMMDDHHMMSSFFFF.log`
- **Per-Job Progress Log Format**: `ffmpeg_h265_progress_job{JobId}_YYYYMMDDHHMMSS.log` (when `-LogVerbose` is enabled)
- **Auto-cleanup**: Logs older than 24 hours or smaller than 2KB are automatically removed
- **Thread-safe**: Mutex-protected logging for parallel operations

## Media Server Integration

The script integrates with Radarr and Sonarr for automatic library updates. Configure the base URLs and API keys in your `ffmpeg_h265.config.json`:

**Example configuration:**
- **Radarr**: `http://192.168.10.15:7878`
- **Sonarr**: `http://192.168.10.15:8989`

**Automatic Media Type Detection:**
The script automatically determines whether to refresh Radarr (Movies) or Sonarr (TV Shows) based on where each file is located:
- Files under `media_path\<movies_subfolder>` trigger Radarr refresh
- Files under `media_path\<tv_shows_subfolder>` trigger Sonarr refresh

**Refresh Timing and Deduplication:**
- Refresh targets are queued during file finalization and deduplicated by type + base URL + title
- The script dispatches Arr refresh requests once at end-of-run (after all file finalization completes)
- This avoids duplicate refresh calls for the same title during large batches while ensuring final library state is refreshed

This allows processing of mixed content. For example, `-Path "V:\Media"` will process both Movies and TV Shows subdirectories, with each file refreshed in the appropriate system based on its actual location.

After processing, the script triggers a refresh scan to update the media server database using the configured base URLs and API keys.

## Troubleshooting

### Script requires PowerShell 7.5 or newer

Upgrade to PowerShell 7.5+: Download from [PowerShell GitHub Releases](https://github.com/PowerShell/PowerShell/releases)

### Hardware encoding fails

- Verify FFmpeg build includes your target encoder support: `ffmpeg -encoders | Select-String "hevc_nvenc|hevc_amf|hevc_qsv"`
- Verify available hardware acceleration backends on this OS: `ffmpeg -hwaccels` (for example `cuda`, `qsv`, `vaapi`, `d3d11va`, `dxva2`, `vulkan`)
- Force a specific backend when needed: `-Encoder nvenc`, `-Encoder amf`, `-Encoder qsv`, or `-Encoder software`
- Update GPU drivers to latest version

### Progress bars not displaying

- Run in PowerShell console (not PowerShell ISE)
- Use `-NoProgress` switch to disable progress bars and use text output instead

### Out of memory errors

- Reduce `-MaxParallelJobs` value
- Ensure sufficient disk space in `ProcessingTemp` directory

### Files not being processed

- Check `-UserFilter` scriptblock isn't excluding files
- Verify `-LastRunDate` isn't filtering out files
- Use `-LogVerbose` to see detailed filtering decisions

## Advanced Customization

All configurable settings are set via `ffmpeg_h265.config.json`. See the **Configuration** section above for details.

**Example ffmpeg_h265.config.json with all settings:**

```json
{
  "ffmpeg_path": "C:\\ffmpeg",
  "processed_path": "V:\\Processed",
  "processing_path": "V:\\ProcessingTemp",
  "media_path": "V:\\Media",
  "radarr_baseUri": "http://192.168.10.15:7878",
  "radarr_apiKey": "your-radarr-api-key",
  "sonarr_baseUri": "http://192.168.10.15:8989",
  "sonarr_apiKey": "your-sonarr-api-key"
}
```

## Performance Tips

1. **Optimal Parallel Jobs**: Start with 3 jobs; monitor GPU usage and adjust accordingly
2. **GPU Limits**: Concurrent session limits depend on your backend and GPU model (NVENC/AMF/QSV)
3. **Storage**: Use SSD for temp directory for better I/O performance
4. **Network Shares**: Avoid encoding over network; copy files locally first
5. **Priority**: Consider lowering process priority if using PC during encoding

## License

This script is provided as-is for personal and educational use.

## Version History

- Initial release: Parallel FFmpeg NVENC H.265 encoding with progress tracking and media server integration
