## Plan: Add Metadata Analysis Mode

Add a `-Analyze` switch parameter that scans the video library, probes each file with ffprobe, collects comprehensive metadata, and outputs to NDJSON format with an index sidecar, then exits without encoding.

**Steps**
1. ✅ Add `-Analyze` and `-HashAlgorithm` (default `md5`) parameters to the param block (parallel with existing switches)
2. ✅ Create `Get-FileMetadata` function to extract comprehensive metadata from probe (including file hash generation, fs metadata, etc.)
3. ✅ Create `Export-MetadataToNDJSON` function to write NDJSON output with stable append semantics and an index (timestamps, offsets) for fast lookup, and optionally generate an HTML report (depends on 2)
4. ✅ Add compaction logic to collapse append-only history into a current-state view (removing deleted records) and update index; run automatically at end of each `-Analyze` run, and support an on-demand `-Compact` mode
5. ✅ Modify main execution logic to branch on `-Analyze`: scan files, probe each, collect metadata, output NDJSON, compact, generate HTML, exit (depends on 4)
6. ✅ Update README.md with new `-Analyze` parameter documentation and output usage (depends on 4)

**Relevant files**
- `ffmpeg_h265.ps1` — Add parameter, functions, and logic branch
- `README.md` — Document new parameter

**Verification**
1. Run script with `-Analyze -Path "test/dir"` and verify NDJSON output contains expected metadata fields
2. Verify index sidecar file is created with file count and paths
3. Confirm no encoding processes start when `-Analyze` is used
4. Test with mixed file types to ensure robust metadata extraction

**Decisions**
- Output format: NDJSON (one JSON object per line) with stable append behavior, plus an index.json sidecar containing timestamps and offsets for fast lookup, and an HTML webpage for awesome visualization of library metadata and analysis decisions
- Metadata scope: Include path, size, timestamps, SHA256 hash, duration, video details (codec, bitrate, resolution, HDR), audio/subtitle arrays, tags
- Schema example:
  ```json
  {
    "path": "Z:/Movies/Inception (2010)/Inception.mkv",
    "size": 7340032000,
    "mtime": "2026-03-16T12:00:00Z",
    "ctime": "2026-03-16T12:00:00Z",
    "hash": "sha256:...",
    "duration": 8880,
    "video": {
      "codec": "h264",
      "bitrate": 8500000,
      "width": 1920,
      "height": 1080,
      "hdr": false
    },
    "audio": [
      { "lang": "eng", "codec": "aac", "channels": 2 },
      { "lang": "jpn", "codec": "aac", "channels": 2 }
    ],
    "subtitles": ["eng", "jpn"],
    "tags": ["movie", "drama"]
  }
  ```
- Execution: Scan-only mode that exits after analysis (no encoding)
- Hashing: Default to MD5 for speed (thousands of files), with optional SHA256 via `-HashAlgorithm` when stronger collision resistance is needed
- Parallel probing: Keep existing parallel ffprobe calls for performance

**Further Considerations**
1. Should the analysis include file system metadata (creation/modification times) in addition to ffprobe data? (Decided: Yes, include mtime/ctime and optionally file size + permissions)
2. How to handle large libraries — add progress reporting during analysis phase? (Decided: Yes, same as encoding scan progress)
3. Should the output include analysis decisions (why files would be encoded/skipped) based on current thresholds? (Decided: Yes, include in JSON and provide awesome HTML webpage for visualization)
4. Fail behavior: support best-effort mode (skip errors) with an optional strict mode to abort on first error (e.g., `-ExitOnError`).
5. Parallel runs prevention: Use a mutex (file-based lock) to ensure only one -Analyze run at a time.
6. Atomic writes: Implement atomic writes by writing to a temporary file and renaming to the final path to prevent partial data corruption.
7. Disk I/O error handling: Wrap all file operations in try/catch blocks to handle disk full, quotas, invalid symlinks, in-flight changes (files moved), invalid filenames, encoding issues, and permission issues; log errors and continue in best-effort mode unless strict mode is enabled.
8. Indexing memory pressure: For large libraries, avoid loading the entire index into memory; use streaming or on-demand loading for lookups to handle memory constraints efficiently.

**Edge Cases and Mitigations**
- Concurrency: Mutex prevents parallel -Analyze runs; for shared access, consider read/write locks if future features allow concurrent reads.
- Partial writes: Atomic rename ensures only complete files are visible.
- Disk issues: Best-effort error handling with logging via existing Write-ParallelLog function (use Warning/Error levels); strict mode for critical environments.
- Memory pressure: Index as JSON array; for very large indexes (>10k entries), consider splitting into chunks or using a more efficient format like binary, but start with JSON for simplicity.
- File changes during scan: Probe files immediately after hashing to minimize stale data; log warnings for moved/deleted files using Write-ParallelLog.
- Encoding issues: Use UTF-8 for NDJSON output; handle invalid characters in paths/metadata by escaping or skipping, logging via Write-ParallelLog.
- Permissions: Check write access to output directory early; skip inaccessible files with warnings logged via Write-ParallelLog.
- Large libraries: Progress reporting and optional resume from last index timestamp.
- Crash recovery: Append-only NDJSON allows resuming from last valid entry; compaction rebuilds clean state.
```

Once you have the plan open and reviewed, if it looks good, please approve it so we can proceed with implementation. If you'd like any changes, let me know.

> NOTE: Keep this plan document synchronized with the implementation; any behavior or schema changes should be reflected here immediately.