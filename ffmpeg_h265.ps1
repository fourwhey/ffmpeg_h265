# ffmpeg_convert_nvenc.ps1
#
# This script automates video conversion using ffmpeg with NVENC hardware acceleration.
# Features include logging, retry logic for file operations, and flexible parameterization.
#
# Usage Example:
#   .\ffmpeg_convert_nvenc.ps1 -Path "C:\Videos" -ResizeRes "1920:1080" -ShowOutputCmd
#
# Requirements:
# - ffmpeg must be installed and accessible at the specified path (default: C:\ffmpeg)
#
# Date: 2021-11-21

Param 
(
    # Path to the file or directory to process
    [Parameter(Mandatory = $true)][string] $Path,
    # Optional root path for organizing output (e.g., movies or tvshows)
    [Parameter(Mandatory = $false, HelpMessage = "movies or tvshows")][string] $RootPath,
    # Show the ffmpeg command before execution
    [Parameter(Mandatory = $false)][switch] $ShowOutputCmd,
    # Show progress during conversion
    [Parameter(Mandatory = $false)][boolean] $ShowProgress = $true,
    # Resize resolution (e.g., 1920:1080)
    [Parameter(Mandatory = $false, HelpMessage = "1920:1080")][string] $ResizeRes = "",
    # Force resizing even if not needed
    [Parameter(Mandatory = $false)][switch] $ForceResize,  
    # Retain aspect ratio when resizing
    [Parameter(Mandatory = $false)][switch] $RetainAspect,
    # Allow upscaling
    [Parameter(Mandatory = $false)][boolean] $CanScaleUp = $false,
    # Allow downscaling
    [Parameter(Mandatory = $false)][boolean] $CanScaleDown = $true,  
    # Constant Quality Parameter for rate control (lower is better quality)
    [Parameter(Mandatory = $false, HelpMessage = "ex. 28, 29. Higher value is worse quality.")][string] $CQPRateControl = 28,
    # Bitrate control (e.g., 2M, 5M)
    [Parameter(Mandatory = $false, HelpMessage = "ex. 2M, 5M. Higher value is better quality.")][string] $BitrateControl = "",
    # Force conversion even if output exists
    [Parameter(Mandatory = $false)][switch] $ForceConvert,
    # Allow reprocessing of files
    [Parameter(Mandatory = $false)][boolean] $CanReprocess = $false,
    # Move files on completion
    [Parameter(Mandatory = $false)][boolean] $MoveOnCompletion = $true,
    # Refresh external apps (e.g., Arr) on completion
    [Parameter(Mandatory = $false)][boolean] $RefreshArrOnCompletion = $true,
    # Enable logging
    [Parameter(Mandatory = $false)][switch] $LogEnabled,
    # Enable verbose logging
    [Parameter(Mandatory = $false)][switch] $LogVerbose,
    # Exit script on error
    [Parameter(Mandatory = $false)][boolean] $ExitOnError = $false,
    # Sorting expression for file processing
    [Parameter(Mandatory = $false)][hashtable] $SortExpression = @{ e = 'Name'; Descending = $false },
    # User-defined filter for file selection
    [Parameter(Mandatory = $false)][scriptblock] $UserFilter = { $_.Length -ne -1 },
    # Preferred audio language
    [Parameter(Mandatory = $false)][string] $AudioLang = "eng",
    # Preferred subtitle language
    [Parameter(Mandatory = $false)][string] $SubLang = "eng",
    # Only process files modified after this date
    [Parameter(Mandatory = $false)][datetime] $LastRunDate,
    # Skip file lock checks
    [Parameter(Mandatory = $false)][switch] $SkipFileLock = $false
)

# Generate a random number between 0 and 30 for a randomized start delay
$randomSeconds = Get-Random -Minimum 0 -Maximum 30
# Countdown to start 
for ( $i = $randomSeconds; $i -ge 0; $i-- )   
{
    Write-Host -NoNewline "$i seconds remaining...$(' ' * ([console]::BufferWidth - "$i seconds remaining...".Length))`r"; 
    Start-Sleep -Seconds 1; 
  
    if (( $i -eq 1 ) -or ( $host.UI.RawUI.KeyAvailable ))
    { 
        Write-Host -NoNewLine "$(' ' * [console]::BufferWidth)`r";
        Write-Host "Starting ffmpeg_convert_nvenc.ps1...$(' ' * ([console]::BufferWidth - "Starting ffmpeg_convert_nvenc.ps1...".Length))";
        break; 
    }
}

# Ensure a fallback if $PSScriptRoot is $null
$script:ScriptRoot = $PSScriptRoot
if (-not $script:ScriptRoot) 
{
    $script:ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
}

# Print the script root directory (with single quotes for clarity)
Write-Host "Script Root: '$($script:ScriptRoot)'"

# Path to ffmpeg binary (customize as needed)
$script:ffmpeg_path = "C:\ffmpeg"
# Log file name and path
$script:logFileName = "ffmpeg_convert_$(Get-Date -Format "yyyyMMddHHmmssffff").log";
$script:logFilePath = Join-Path $script:ScriptRoot $script:logFileName;

# LogLevel enum for structured logging
enum LogLevel
{
    Information = 0
    Warning = 1
    Error = 2
    None = 3
}

enum OutputTarget
{
    None = 0
    File = 1
    Host = 2
    Both = 3
}

function Write-Log
{
    [CmdletBinding()]
    param
    (
        [Parameter(Mandatory = $true, Position = 0, ValueFromPipeline = $true)] [string]$Message,
        [Parameter(Mandatory = $false)] [LogLevel]$Level = [LogLevel]::Information,
        [Parameter(Mandatory = $false)] [OutputTarget]$Target = [OutputTarget]::Both,
        [Parameter(Mandatory = $false)] [string]$LogPath = $script:logFilePath,
        [Parameter(Mandatory = $false)] [switch]$NoTimestamp,
        [Parameter(Mandatory = $false)] [switch]$HostNoNewLine
    )

    begin
    {
        # Validation
        if (($Target -band [OutputTarget]::File) -and -not $LogPath)
        {
            throw "LogPath is required when Target includes File output"
        }
    }

    process
    {
        $timestamp = ($NoTimestamp) ? '' : "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss.fff') - " 
    
        $formattedMessage = switch ($Level)
        {
            ([LogLevel]::Error)
            {
                "$timestamp[ERROR] $Message" 
            }
            ([LogLevel]::Warning)
            {
                "$timestamp[WARNING] $Message" 
            }
            default
            {
                "$timestamp$Message" 
            }
        }

        # Write to file if requested
        if ($Target -band [OutputTarget]::File)
        {
            try
            {
                Add-Content -Path $LogPath -Value $formattedMessage -ErrorAction Stop
            }
            catch
            {
                Write-Warning "Failed to write to log file: $_"
                # If we can't write to file but host output is enabled, continue
                if ( -not ( $Target -band [OutputTarget]::Host ))
                {
                    throw
                }
            }
        }

        # Write to host if requested
        if ( $Target -band [OutputTarget]::Host )
        {    
            if ( -not $HostNoNewline )
            {
                $cp = [console]::GetCursorPosition();
                if ( $cp.Item1 -gt 0 )
                {
                    $Message = "`r`n$($Message)";
                }
                else 
                {
                    $Message = "`n$($Message)";
                }
            }

            $WriteHostParams = @{ 
                Object    = $Message
                NoNewline = $HostNoNewLine
            }

            switch ($Level)
            {
                ([LogLevel]::Error)
                { 
                    $WriteHostParams += @{ ForegroundColor = "Red" }
                    Write-Host @WriteHostParams
                }
                ([LogLevel]::Warning)
                {
                    $WriteHostParams += @{ ForegroundColor = "Yellow" }
                    Write-Host @WriteHostParams
                }
                default
                {
                    Write-Host @WriteHostParams
                }
            }
        }
    }
}

# Function to perform Rename-Item with retry logic
function Rename-WithRetry
{
    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
    param (
        [Parameter(Mandatory = $true, Position = 0, ValueFromPipeline = $true)]
        [string]$LiteralPath, # Mandatory path to the item
        [Parameter(Mandatory = $true, Position = 1)]
        [string]$NewName,
        [int] $MaxRetries = 3, # Number of retries
        [int] $RetryDelay = 10  # Delay in seconds between retries
    )

    $Attempt = 0
    $Success = $false

    # Create new hashtable with only the parameters Rename-Item accepts
    $RenameParams = @{
        LiteralPath = $LiteralPath
        NewName     = $NewName
    }
    # if ($WhatIfPreference)
    # {
    #   $RenameParams.WhatIf = $true 
    # }
    # if ($ConfirmPreference)
    # {
    #   $RenameParams.Confirm = $true 
    # }

    while (-not $Success -and $Attempt -lt $MaxRetries)
    {
        try
        {
            Rename-Item @RenameParams
            Write-Log -Message "Renamed: $LiteralPath to $NewName" -Target Host
            $Success = $true
        }
        catch
        {
            $Attempt++
            Write-Log -Message "Failed to rename $LiteralPath. Attempt $Attempt of $MaxRetries. `r`nError: $($Error[0].Exception.Message)" -Level Warning -Target File
            if ($Attempt -lt $MaxRetries)
            {
                Write-Log -Message "Retrying in $RetryDelay seconds..."
                Start-Sleep -Seconds $RetryDelay
            }
            else
            {
                Write-Log -Message "Max retries reached. Could not rename $LiteralPath." -Level Error
            }
        }
    }
}

# Function to perform Move-Item with retry logic
function Move-WithRetry
{
    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
    param (
        [Parameter(Mandatory = $true, Position = 0, ValueFromPipeline = $true)] [string]$LiteralPath, # Mandatory source path of the item to move
        [Parameter(Mandatory = $true, Position = 1)] [string]$Destination, # Mandatory target destination path
        [switch]$Force, # Force support
        [int] $MaxRetries = 3, # Number of retries
        [int] $RetryDelay = 10, # Delay in seconds between retries
        [switch] $LogOnSuccess
    )

    $Attempt = 0
    $Success = $false

    # Create new hashtable with only the parameters Move-Item accepts
    $MoveParams = @{
        LiteralPath = $LiteralPath
        Destination = $Destination
    }

    if ($Force)
    {
        $MoveParams.Force = $true 
    }
    # if ($WhatIfPreference)
    # {
    #   $MoveParams.WhatIf = $true 
    # }
    # if ($ConfirmPreference)
    # {
    #   $MoveParams.Confirm = $true 
    # }

    while (-not $Success -and $Attempt -lt $MaxRetries)
    {
        try
        {
            $Attempt++
            Move-Item @MoveParams -ErrorAction Stop
            $Success = $true
            if ($LogOnSuccess)
            {
                Write-Log -Message "Moved $LiteralPath`r`nTo: $Destination" -Level Information
            }
        }
        catch
        {      
            Write-Log -Message "Failed to move $LiteralPath. Attempt $Attempt of $MaxRetries. `r`nError: $($Error[0].Exception.Message)" -Level Warning
            if ($Attempt -lt $MaxRetries)
            {
                Write-Log -Message "Retrying in $RetryDelay seconds..."
                Start-Sleep -Seconds $RetryDelay
            }
            else
            {
                Write-Log -Message "Max retries reached. Could not move $LiteralPath." -Level Error
        
                try
                {
                    Get-Item -Path $LiteralPath -ErrorAction Stop | Out-Null;

                    # Write the failed_move log so it can be retried on next launch.
                    $MoveParamsJson = $MoveParams | ConvertTo-Json -Depth 1;
                    $fileNameWithoutExt = [System.IO.Path]::GetFileNameWithoutExtension($LiteralPath);
                    $logPath = Join-Path $script:ScriptRoot "failed_move_$($fileNameWithoutExt).log";
          
                    Write-Log -Message "$($MoveParamsJson)" -Level None -Target File -NoTimeStamp -LogPath $logPath;
                }
                catch
                {
                    Write-Log -Message "Could not create failed_move log. Error: $($_.Exception.Message)" -Level Error
                }
            }
        }
    }
}

# Function to retry failed move files
function Resume-FailedMoves
{
    # Retry failed moves.
    Get-ChildItem -Path $script:ScriptRoot -Filter "failed_move_*" | ForEach-Object {
        $CurrentFile = $_; # Store the pipeline object in a variable
        if ( $CurrentFile | Test-Path -ErrorAction SilentlyContinue )
        {
            Write-Log -Message "Attempting to lock failed_move file: $($CurrentFile.FullName)" -Level Information;
            try 
            {
                # Attempt to open the failed_move log file with an exclusive read lock.
                $content = '';

                try 
                {
                    # Create FileStream
                    $fs = [System.IO.FileStream]::new(
                        $CurrentFile,
                        [System.IO.FileMode]::Open,
                        [System.IO.FileAccess]::Read,
                        [System.IO.FileShare]::None
                    );

                    # Create StreamReader
                    $sr = [System.IO.StreamReader]::new( $fs );

                    # Read the file contents
                    $content = $sr.ReadToEnd();
                }
                finally 
                {
                    # Dispose of resources
                    if ( $sr )
                    {
                        $sr.Dispose(); 
                    }
          
                    if ( $fs )
                    {
                        $fs.Dispose(); 
                    }
                }

                if ( $content )
                {
                    # Remove the failed_move log file
                    $CurrentFile | Remove-WithRetry -Force;

                    # Parse the file contents as JSON
                    $MoveParams = $content | ConvertFrom-Json;

                    if ( $MoveParams )
                    {
                        # Attempt to move the file in $MoveParams
                        Move-WithRetry -LiteralPath $MoveParams.LiteralPath -Destination $MoveParams.Destination -LogOnSuccess;
                    }
                }
            }
            catch 
            {
                Write-Log -Message "Failed to process failed_move file: $($CurrentFile.FullName)`r`nException: $($Error[0].Exception.Message)" -Level Warning;
            }
        }
    }
}

# Function to perform Remove-Item with retry logic
function Remove-WithRetry
{
    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
    param (
        [Parameter(Mandatory = $true, Position = 0, ValueFromPipeline = $true)] [string]$LiteralPath, # Mandatory path to the item
        [switch]$Force, # Force support
        [int] $MaxRetries = 3, # Number of retries
        [int] $RetryDelay = 10  # Delay in seconds between retries
    )

    $Attempt = 0
    $Success = $false

    # Create new hashtable with only the parameters Remove-Item accepts
    $RemoveParams = @{
        LiteralPath = $LiteralPath
    }
    if ($Force)
    {
        $RemoveParams.Force = $true 
    }

    while (-not $Success -and $Attempt -lt $MaxRetries)
    {
        try
        {
            $Attempt++
            if (Test-Path -LiteralPath $LiteralPath -ErrorAction SilentlyContinue)
            {
                Remove-Item @RemoveParams -ErrorAction Stop
                Write-Log -Message "Removed: $LiteralPath"
                $Success = $true
            }
            else
            {
                Write-Log -Message "File does not exist: $LiteralPath. Skipping." -Level Warning
                $Success = $true
            }
        }
        catch
        {
            Write-Log -Message "Failed to remove $LiteralPath. Attempt $Attempt of $MaxRetries. `r`nError: $($Error[0].Exception.Message)" -Level Warning
            if ($Attempt -lt $MaxRetries)
            {
                Write-Log -Message "Retrying in $RetryDelay seconds..."
                Start-Sleep -Seconds $RetryDelay
            }
            else
            {
                Write-Log -Message "Max retries reached. Could not remove $LiteralPath." -Level Error
            }
        }
    }
}

function ParseStreamInfo
{
    param (
        [Parameter(Mandatory = $true, Position = 0)] [string]$streamInfo,
        [Parameter(Mandatory = $true, Position = 1)] [ValidateSet("audio", "video", "subtitle")] [string]$streamType
    )

    # Split $streamInfo into $lines, excluding empty lines.
    $lines = $streamInfo -split "`r?`n";
    $lines = $lines | Where-Object { $_.Trim() -ne "" };

    # Initialize an empty list to hold the stream objects
    $streams = @();

    # Initialize an empty hashtable to hold stream properties
    $stream = @{};
    $insideStreamBlock = $false;
    
    # Iterate through each line
    foreach ($line in $lines)
    {
        # Trim the line
        $line = $line.Trim();

        # Check for stream start tag
        if ($line -ieq "[STREAM]")
        {
            $insideStreamBlock = $true;
            $stream = @{};
            continue;
        }

        # Check for stream end tag
        if ($line -ieq "[/STREAM]")
        {
            # End of stream block reached. Process $stream by $streamType.
            $insideStreamBlock = $false;
            
            switch ($streamType)
            {
                "audio"
                {
                    $streams += [PSCustomObject]@{
                        Index                      = $stream["index"]
                        CodecType                  = $stream["codec_type"]
                        CodecName                  = $stream["codec_name"]
                        Language                   = $stream["language"]
                        Title                      = $stream["title"]
                        DispositionDefault         = $stream["disposition_default"]
                        DispositionDub             = $stream["disposition_dub"]
                        DispositionOriginal        = $stream["disposition_original"]
                        DispositionComment         = $stream["disposition_comment"]
                        DispositionLyrics          = $stream["disposition_lyrics"]
                        DispositionKaraoke         = $stream["disposition_karaoke"]
                        DispositionForced          = $stream["disposition_forced"]
                        DispositionHearingImpaired = $stream["disposition_hearing_impaired"]
                        DispositionVisualImpaired  = $stream["disposition_visual_impaired"]
                        DispositionCleanEffects    = $stream["disposition_clean_effects"]
                        DispositionAttachedPic     = $stream["disposition_attached_pic"]
                        DispositionTimedThumbnails = $stream["disposition_timed_thumbnails"]
                        DispositionNonDiegetic     = $stream["disposition_non_diegetic"]
                        DispositionCaptions        = $stream["disposition_captions"]
                        DispositionDescriptions    = $stream["disposition_descriptions"]
                        DispositionMetadata        = $stream["disposition_metadata"]
                        DispositionDependent       = $stream["disposition_dependent"]
                        DispositionStillImage      = $stream["disposition_still_image"]
                        DispositionMultilayer      = $stream["disposition_multilayer"]
                    };
                }                
                "video"
                {
                    $streams += [PSCustomObject]@{
                        Index                      = $stream["index"]
                        CodecName                  = $stream["codec_name"]
                        CodecLongName              = $stream["codec_long_name"]
                        Profile                    = $stream["profile"]
                        CodecType                  = $stream["codec_type"]
                        CodecTagString             = $stream["codec_tag_string"]
                        CodecTag                   = $stream["codec_tag"]
                        Width                      = $stream["width"]
                        Height                     = $stream["height"]
                        CodedWidth                 = $stream["coded_width"]
                        CodedHeight                = $stream["coded_height"]
                        ClosedCaptions             = $stream["closed_captions"]
                        FilmGrain                  = $stream["film_grain"]
                        HasBFrames                 = $stream["has_b_frames"]
                        SampleAspectRatio          = $stream["sample_aspect_ratio"]
                        DisplayAspectRatio         = $stream["display_aspect_ratio"]
                        PixFmt                     = $stream["pix_fmt"]
                        Level                      = $stream["level"]
                        ColorRange                 = $stream["color_range"]
                        ColorSpace                 = $stream["color_space"]
                        ColorTransfer              = $stream["color_transfer"]
                        ColorPrimaries             = $stream["color_primaries"]
                        ChromaLocation             = $stream["chroma_location"]
                        FieldOrder                 = $stream["field_order"]
                        Refs                       = $stream["refs"]
                        ViewIdsAvailable           = $stream["view_ids_available"]
                        ViewPosAvailable           = $stream["view_pos_available"]
                        Id                         = $stream["id"]
                        RFrameRate                 = $stream["r_frame_rate"]
                        AvgFrameRate               = $stream["avg_frame_rate"]
                        TimeBase                   = $stream["time_base"]
                        StartPts                   = $stream["start_pts"]
                        StartTime                  = $stream["start_time"]
                        DurationTS                 = $stream["duration_ts"]
                        Duration                   = $stream["duration"]
                        BitRate                    = $stream["bit_rate"]
                        MaxBitRate                 = $stream["max_bit_rate"]
                        BitsPerRawSample           = $stream["bits_per_raw_sample"]
                        NbFrames                   = $stream["nb_frames"]
                        NbReadFrames               = $stream["nb_read_frames"]
                        NbReadPackets              = $stream["nb_read_packets"]
                        ExtraDataSize              = $stream["extradata_size"]
                        Language                   = $stream["language"]
                        BPS                        = $stream["BPS-eng"]
                        DurationEng                = $stream["DURATION-eng"]
                        NumberOfFramesEng          = $stream["NUMBER_OF_FRAMES-eng"]
                        NumberOfBytesEng           = $stream["NUMBER_OF_BYTES-eng"]
                        SourceID                   = $stream["SOURCE_ID-eng"]
                        StatisticsWritingApp       = $stream["_STATISTICS_WRITING_APP-eng"]
                        StatisticsWritingDateUTC   = $stream["_STATISTICS_WRITING_DATE_UTC-eng"]
                        StatisticsTags             = $stream["_STATISTICS_TAGS-eng"]
                        DispositionDefault         = $stream["disposition_default"]
                        DispositionDub             = $stream["disposition_dub"]
                        DispositionOriginal        = $stream["disposition_original"]
                        DispositionComment         = $stream["disposition_comment"]
                        DispositionLyrics          = $stream["disposition_lyrics"]
                        DispositionKaraoke         = $stream["disposition_karaoke"]
                        DispositionForced          = $stream["disposition_forced"]
                        DispositionHearingImpaired = $stream["disposition_hearing_impaired"]
                        DispositionVisualImpaired  = $stream["disposition_visual_impaired"]
                        DispositionCleanEffects    = $stream["disposition_clean_effects"]
                        DispositionAttachedPic     = $stream["disposition_attached_pic"]
                        DispositionTimedThumbnails = $stream["disposition_timed_thumbnails"]
                        DispositionNonDiegetic     = $stream["disposition_non_diegetic"]
                        DispositionCaptions        = $stream["disposition_captions"]
                        DispositionDescriptions    = $stream["disposition_descriptions"]
                        DispositionMetadata        = $stream["disposition_metadata"]
                        DispositionDependent       = $stream["disposition_dependent"]
                        DispositionStillImage      = $stream["disposition_still_image"]
                        DispositionMultilayer      = $stream["disposition_multilayer"]
                    };
                    continue;
                }
                "subtitle"
                {
                    $streams += [PSCustomObject]@{
                        Index                      = $stream["index"]
                        CodecType                  = $stream["codec_type"]
                        CodecName                  = $stream["codec_name"]
                        Language                   = $stream["language"]
                        Title                      = $stream["title"]
                        DispositionDefault         = $stream["disposition_default"]
                        DispositionDub             = $stream["disposition_dub"]
                        DispositionOriginal        = $stream["disposition_original"]
                        DispositionComment         = $stream["disposition_comment"]
                        DispositionLyrics          = $stream["disposition_lyrics"]
                        DispositionKaraoke         = $stream["disposition_karaoke"]
                        DispositionForced          = $stream["disposition_forced"]
                        DispositionHearingImpaired = $stream["disposition_hearing_impaired"]
                        DispositionVisualImpaired  = $stream["disposition_visual_impaired"]
                        DispositionCleanEffects    = $stream["disposition_clean_effects"]
                        DispositionAttachedPic     = $stream["disposition_attached_pic"]
                        DispositionTimedThumbnails = $stream["disposition_timed_thumbnails"]
                        DispositionNonDiegetic     = $stream["disposition_non_diegetic"]
                        DispositionCaptions        = $stream["disposition_captions"]
                        DispositionDescriptions    = $stream["disposition_descriptions"]
                        DispositionMetadata        = $stream["disposition_metadata"]
                        DispositionDependent       = $stream["disposition_dependent"]
                        DispositionStillImage      = $stream["disposition_still_image"]
                        DispositionMultilayer      = $stream["disposition_multilayer"]
                        CodecLongName              = $stream["codec_long_name"]
                        Profile                    = $stream["profile"]
                        CodecTagString             = $stream["codec_tag_string"]
                        CodecTag                   = $stream["codec_tag"]
                        Width                      = $stream["width"]
                        Height                     = $stream["height"]
                        ID                         = $stream["id"]
                        RFrameRate                 = $stream["r_frame_rate"]
                        AvgFrameRate               = $stream["avg_frame_rate"]
                        TimeBase                   = $stream["time_base"]
                        StartPts                   = $stream["start_pts"]
                        StartTime                  = $stream["start_time"]
                        DurationTs                 = $stream["duration_ts"]
                        Duration                   = $stream["duration"]
                        BitRate                    = $stream["bit_rate"]
                        MaxBitRate                 = $stream["max_bit_rate"]
                        BitsPerRawSample           = $stream["bits_per_raw_sample"]
                        NbFrames                   = $stream["nb_frames"]
                        NbReadFrames               = $stream["nb_read_frames"]
                        NbReadPackets              = $stream["nb_read_packets"]
                        ExtraDataSize              = $stream["extradata_size"]
                        BPS                        = $stream["BPS"]
                        NumberOfFrames             = $stream["NUMBER_OF_FRAMES"]
                        NumberOfBytes              = $stream["NUMBER_OF_BYTES"]
                        StatisticsWritingApp       = $stream["_STATISTICS_WRITING_APP"]
                        StatisticsWritingDateUTC   = $stream["_STATISTICS_WRITING_DATE_UTC"]
                        StatisticsTags             = $stream["_STATISTICS_TAGS"]
                        Encoder                    = $stream["ENCODER"]
                        DurationTag                = $stream["DURATION"]
                    };
                    continue;
                }
                default
                { 
                    throw "Unknown stream type encountered: $streamType. Expected 'audio', 'video', or 'subtitle'."; 
                }                
            }
        }
        
        # If inside a stream block, parse store key-value pairs in $stream.
        if ($insideStreamBlock)
        {
            # Split the line into key and value
            $key, $value = $line -split "=", 2;

            # Handle different tags and dispositions
            if ($key -ilike "DISPOSITION:*")
            {
                $dispositionKey = $key -replace "DISPOSITION:", "";
                $stream["disposition_$dispositionKey"] = $value;
            }
            elseif ($key -ilike "TAG:*")
            {
                $tagKey = $key -replace "TAG:", "";
                $stream[$tagKey] = $value;
            }
            else
            {
                # Add the key-value pair to the current stream hashtable
                $stream[$key] = $value;
            }
        }
    }

    # Return list of stream objects
    return $streams
}

enum ResTypes
{
    UNKNOWN = 0
    SD = 1
    DVD = 2
    HD = 3
    FHD = 4
    QHD = 5
    UHD_4K = 6
    FUHD_8K = 7
}

enum ArrType
{
    movie
    series
}

# Try and clean up old log files.
$logFilter = "$($script:ScriptRoot)\ffmpeg_convert*.log";

# Get logs that are at least 1 hour old.
$logs = Get-Item -Path $logFilter -Exclude $script:logFileName | Where-Object { $_.CreationTime -lt (Get-Date).AddMinutes(-60) };

foreach ( $log in $logs )
{
    $logSplit = ($log.BaseName -Split "_");
    if ( $logSplit.Count -eq 3 )
    {
        $today = (Get-Date).Date;
        $logday = [DateTime]::ParseExact($logSplit[2].SubString(0, 8), "yyyyMMdd", $null);

        # Remove old files or ones with no content.
        if (( $logday -lt $today.AddDays(-1) ) -or ( $log.Length -lt 2KB ))
        {
            Remove-WithRetry -LiteralPath $log.FullName -Force;
        }
    }
}

Resume-FailedMoves;

# Stop Plex Transcoder processes that have run for more than 1.5 hours.
# These are typically hung processes and can be ended without issue.
Get-Process | Where-Object { 
    $_.Name -ilike "plex trans*" -and (New-TimeSpan -Start $_.StartTime -End (Get-Date)).TotalHours -gt 1.5 
} | ForEach-Object {
    Write-Log "Stopping long-running Plex Transcoder process (PID: $($_.Id), Runtime: $((New-TimeSpan -Start $_.StartTime -End (Get-Date)).TotalHours) hours)"
    $_ | Stop-Process -Force -ErrorAction SilentlyContinue
};

# Escape the path literal
$Path = [Management.Automation.WildcardPattern]::Escape($Path);

function GetResType ( $ResW, $ResH )
{
    #$AR = [math]::Round($ResW / $ResH, 2);

    if (( $ResW -eq 0 ) -or ( $ResH -eq 0 ))
    {
        return [ResTypes]::UNKNOWN;
    }
    elseif (( $ResW -le 500 ) -and ( $ResH -le 400 ))
    { 
        return [ResTypes]::SD;
    }
    elseif (( $ResW -ge 500 ) -and ( $ResH -le 600 ))
    { 
        return [ResTypes]::DVD;
    }
    elseif (( $ResW -ge 900 ) -and ( $ResH -lt 780 ))
    {
        return [ResTypes]::HD;
    }
    elseif (( $ResW -ge 1400 ) -and ( $ResH -lt 1400 ))
    {
        return [ResTypes]::FHD;
    }
    elseif (( $ResW -ge 2000 ) -and ( $ResH -lt 1700 ))
    {
        return [ResTypes]::QHD;
    }
    elseif (( $ResW -ge 3000 ) -and ( $ResH -le 2160 ))
    {
        return [ResTypes]::UHD_4K;
    }
    elseif ( $ResH -gt 2160 )
    {
        return [ResTypes]::FUHD_8K;
    }
    else
    {
        return [ResTypes]::UNKNOWN;
    }
}

function CheckFileStatus
{
    param (
        [Parameter(Mandatory = $true, Position = 0, ValueFromPipeline = $true)]
        [string]$filePath
    )

    if ( $SkipFileLock )
    {
        return "[S]"; # Skipped
    }

    try
    {
        if (Test-Path -LiteralPath $filePath)
        {
            try
            {
                # Attempt to open the file for writing
                $fs = [System.IO.FileStream]::new(
                    $filePath, 
                    [System.IO.FileMode]::Open, 
                    [System.IO.FileAccess]::Read, 
                    [System.IO.FileShare]::None
                );
                $fs.Close();
                return "[W]"; # File is writable
            }
            catch
            {
                Write-Log -Message "Failed to lock, the file is in use: $filePath" -Target File;
                return "[L]"; # File is locked or cannot be written
            }
        }
        else
        {
            Write-Log -Message "File does not exist: $($filePath)`r`nStackTrace: $($Error[0].Exception.StackTrace)" -Level Error;
            return "[NF]"; # File does not exist
        }
    }
    catch
    {
        Write-Log -Message "Error accessing file: $filePath. Error details: $($Error[0].Exception.Message)" -Target File;
        return "[E]"; # Error when testing file
    }
}

function RefreshArrTitles
{
    param 
    ( 
        [Parameter(Mandatory = $true, Position = 0)] [ArrType] $type,
        [Parameter(Mandatory = $true, Position = 1)] [int[]] $titleIds, 
        [Parameter(Mandatory = $true, Position = 2)] [string] $baseUri, 
        [Parameter(Mandatory = $true, Position = 3)] [string] $apiKey 
    )
    $session = New-Object Microsoft.PowerShell.Commands.WebRequestSession;
    $session.UserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0";

    if ( $type -eq [ArrType]::movie )
    {
        $body = "{`"name`":`"RefreshMovie`",`"movieIds`":[$($titleIds -Join ",")]}";
    }
    else 
    {
        $body = "{`"name`":`"RefreshSeries`",`"seriesId`":$($titleIds[0])}";
    }

    try
    {
        Invoke-WebRequest -UseBasicParsing -Uri "$($baseUri)/api/v3/command" `
            -Method "POST" `
            -WebSession $session `
            -Headers @{
            "Accept"           = "application/json, text/javascript, */*; q=0.01"
            "Accept-Encoding"  = "gzip, deflate"
            "Accept-Language"  = "en-US,en;q=0.9"
            "Cache-Control"    = "no-cache"
            "DNT"              = "1"
            "Origin"           = "$($baseUri)"
            "Pragma"           = "no-cache"
            "X-Api-Key"        = "$($apiKey)"
            "X-Requested-With" = "XMLHttpRequest"
        } `
            -ContentType "application/json" `
            -Body "$($body)" | Out-Null;
    }
    catch
    {
        Write-Log -Message $Error[0].Exception.Message;
        Write-Log -Message $Error[0].Exception.StackTrace;
    }
}

function GetArrTitleIds()
{  
    param
    (
        [Parameter(Mandatory = $true, Position = 0)] [ArrType] $type,
        [Parameter(Mandatory = $true, Position = 1)] [System.IO.DirectoryInfo] $dirInfo, 
        [Parameter(Mandatory = $true, Position = 2)] [string] $baseUri, 
        [Parameter(Mandatory = $true, Position = 3)] [string] $apiKey
    )
    # Use $dirInfo for title lookup.
    #  $type = [ArrType]::series ex. "\\localhost\v\media\tv shows\big buck bunny\season 01" -- Use Directory.Parent.Name or "big buck bunny" as $titleName.
    #  $type = [ArrType]::movie  ex. "\\localhost\v\media\movies\big buck bunny"             -- Use Directory.Name        or "big buck bunny" as $titleName.
    $titleName = (($type -eq [ArrType]::series -or $dirInfo.Name.ToLower().StartsWith("season")) ? $dirInfo.Parent.Name : $dirInfo.Name);

    # UrlEncode $titleName.
    $term = [System.Web.HttpUtility]::UrlEncode($titleName);
  
    $session = New-Object Microsoft.PowerShell.Commands.WebRequestSession;
    $session.UserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0";
    try 
    {
        $response = Invoke-WebRequest -UseBasicParsing -Uri "$($baseUri)/api/v3/$([ArrType].GetEnumName($type))/lookup?term=$($term)" `
            -Method "GET" `
            -WebSession $session `
            -Headers @{
            "Accept"    = "*/*"
            "X-Api-Key" = "$($apiKey)"
        } `
            -ContentType "application/json" | ConvertFrom-Json | Where-Object { ( -not [string]::IsNullOrWhiteSpace( $_.path )) };   

        return $response.id;
    }
    catch 
    {
        Write-Log -Message $Error[0].Exception.Message;
        Write-Log -Message $Error[0].Exception.StackTrace;
        return @();
    }
}

# function GetCQPRateControl
# {
#   $RateCtrl = $CQPRateControl;
  
#   if ( $CQPRateCtrl -eq 0 )
#   {
#     if ( (($impm - $tmpm) / 4) -ge 1 )
#     {
#       $RateCtrl = 28 + [int](($impm - $tmpm) / 10);
#     }
#     $RateCtrl = (( $RateCtrl -gt 29 ) ? (( $ismb -ge 1000 ) ? 29 : 28 ) : $RateCtrl);
#   }
#   Write-Host "`r`nRc: $($RateCtrl), Src: $($SrcResType), Res: $($SrcResW):$($SrcResH), iMB: $($ismb), iMB/min: $($impm), tMB/min: $($tmpm)" -NoNewline;

#   return "-qp:v $($RateCtrl)";
# }

# function GetBitrateControl
# {
#   $RateCtrl = $BitrateControl;
  
#   if ( $RateCtrl -ne "" )
#   {
#     $RateCtrl = "-b:v $($RateCtrl)";
#     Write-Host ", Bitrate: $($BitrateControl)";
#   }
#   else 
#   {
#     Write-Host
#   }
  
#   return $RateCtrl;
# }

# Function to get video duration using ffprobe
function Get-VideoDuration 
{
    param ($filePath)
    $duration = & "$($script:ffmpeg_path)\ffprobe.exe" -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "$filePath"
    return [TimeSpan]::FromSeconds([double]$duration)
}

function ShowSummary
(
    $InputDurationMin, $InputFileSizeMB, $OutputFileSizeMB, $InputMBPerMin, $OutputMBPerMin, 
    $SrcVidCodec, $DstVidCodec, $SrcVidProfile, $DstVidProfile, $SrcVidBit, $DstVidBit, 
    $SrcAudCodec, $DstAudCodec
)
{
    $d10 = $InputDurationMin.ToString().Trim().PadRight(10, [char]32);
    $d12 = $InputDurationMin.ToString().Trim().PadRight(12, [char]32);
    $ifs10 = $InputFileSizeMB.ToString().Trim().PadRight(10, [char]32);
    $ofs12 = $OutputFileSizeMB.ToString().Trim().PadRight(12, [char]32);
    $imb10 = $InputMBPerMin.ToString().Trim().PadRight(10, [char]32);
    $omb12 = $OutputMBPerMin.ToString().Trim().PadRight(12, [char]32);
    $ivcd10 = $SrcVidCodec.Trim().PadRight(10, [char]32);
    $ovcd19 = $DstVidCodec.Trim().PadRight(19, [char]32);
    $ivp10 = $SrcVidProfile.Trim().PadRight(10, [char]32);
    $ovp19 = $DstVidProfile.Trim().PadRight(19, [char]32);
    $idp10 = $SrcVidBit.Trim().PadRight(10, [char]32);
    $odp19 = $DstVidBit.Trim().PadRight(19, [char]32);
    $iacd10 = $SrcAudCodec.Trim().PadRight(10, [char]32).ToUpper();
    $oacd19 = $DstAudCodec.Trim().PadRight(19, [char]32).ToUpper();
  
    if ( $DstAudCodec -ieq "copy" )
    {
        $oacd19 = $iacd10.PadRight(19, [char]32).ToUpper();
    }

    Write-Host 
    Write-Host "┌──────────────────────┬─────────────────────┐" -BackgroundColor Black;
    Write-Host "│             (input)  │ (output)            │" -NoNewLine -BackgroundColor DarkGray; Write-Host "";
    Write-Host "├─────────┬────────────┼─────────────────────┤" -BackgroundColor Black;
    Write-Host "│ Length  │ $($d10   ) │ $($d12     ) Min(s) │" -BackgroundColor Black;
    Write-Host "│ Size    │ $($ifs10 ) │ $($ofs12   )     MB │" -BackgroundColor Black;
    Write-Host "│ MB/Min  │ $($imb10 ) │ $($omb12   ) MB/Min │" -BackgroundColor Black;
    Write-Host "├─────────┼────────────┼─────────────────────┤" -BackgroundColor Black;
    Write-Host "│ VCodec  │ $($ivcd10) │ $($ovcd19         ) │" -BackgroundColor Black;
    Write-Host "│ Profile │ $($ivp10 ) │ $($ovp19          ) │" -BackgroundColor Black;
    Write-Host "│ Depth   │ $($idp10 ) │ $($odp19          ) │" -BackgroundColor Black;
    Write-Host "│ ACodec  │ $($iacd10) │ $($oacd19         ) │" -BackgroundColor Black;
    Write-Host "└─────────┴────────────┴─────────────────────┘" -BackgroundColor Black;
}

Write-Log -Message "Start Logging Process" -Target File;

$processed_path = "V:\Processed";
$processing_temp = "V:\ProcessingTemp\";
$media_path = "V:\Media";
$tv_shows_path = "$($media_path)\TV Shows";
$movies_path = "$($media_path)\Movies";
$dvcdn = "hevc";

$DestResW = 0;
$DestResH = 0;
$DestIsResMatched = $false;
$DestResType = [ResTypes]::UNKNOWN;

if ( $ResizeRes.ToLower().Split(':').Count -eq 2 )
{
    if 
    (
        ([int]::TryParse( $ResizeRes.ToLower().Split(':')[0].Trim(), [ref]$DestResW )) -and 
        ([int]::TryParse( $ResizeRes.ToLower().Split(':')[1].Trim(), [ref]$DestResH ))
    )
    {
        $DestResW = ( -not $CanScaleDown ) ? [Math]::Max(1920, [Math]::Min(1920, $DestResW)) : $DestResW ;
        $DestResH = ( -not $CanScaleDown ) ? [Math]::Max(1080, [Math]::Min(1080, $DestResH)) : $DestResH ;
        $DestIsResMatched = $true;

        $DestResType = GetResType $DestResW $DestResH;
    }
}

$ResizeRes = "";

if ( $RootPath.ToLower() -ieq 'movies' )
{
    $Path = "$($movies_path)\$($Path)";
}
elseif ( $RootPath.ToLower() -ieq 'tvshows' )
{
    $Path = "$($tv_shows_path)\$($Path)";
}

if ( $Path.Contains( $movies_path ) )
{
    # Radarr
    $type = [ArrType]::movie;
    $baseUri = "http://192.168.10.15:7878";
    $apiKey = "f874f5792f624d26912d8e7e51b8159c";
}
else 
{
    # Sonarr
    $type = [ArrType]::series;
    $baseUri = "http://192.168.10.15:8989";
    $apiKey = "76c51b5a71aa4b9b840b0292a14c059a";
}

# to exclude "- proc", return $true when $_.Name notmatch "- proc"
$ReprocessFilter = { $_.Name -notmatch "- proc" }; 
if ( $CanReprocess -eq $true )   
{
    # return $true for everything so it will include "- proc"
    $ReprocessFilter = { $true };
}

# SortExpression
# @{ e='Name'; Descending=$false }
# @{ e='CreationTime'; Descending=$true }
# @{ e='LastAccessTime'; Descending=$true }
# @{ e='LastWriteTime'; Descending=$true }
# @{ e='Length'; Descending=$true }

# UserFilter
# { $_.Name -like '*identity*' }
# { $_.CreationTime -gt (Get-Date).AddDays(-2) }
# { $_.LastAccessTime -gt (Get-Date).AddDays(-2) }
# { $_.LastWriteTime -gt (Get-Date).AddDays(-2) }

try
{
    Get-ChildItem -LiteralPath "$($Path)" -Recurse | 
    # User-defined sort.
    Sort-Object $SortExpression |
    # Default file extension filter.
    Where-Object { $_.Extension -match "^\.(avi|divx|m.*|ts|wmv)" } |
    # Reprocess Filter.
    Where-Object -FilterScript $ReprocessFilter |
    # User-defined filter.
    Where-Object -FilterScript $UserFilter | 
    ForEach-Object {
        $IsReprocess = $false;
        $new_file = "";
        $BaseName = "";
    
        if 
        (
            ( $LastRunDate -ne $null ) -and 
            ( $_.CreationTime.Date -lt $LastRunDate.Date )
        )
        {
            return;
        }    

        # Skip processed files.
        if ( $_.Name -Match "- proc" )
        {
            $IsReprocess = $true;      
        }

        $BasePath = "$($_.Directory)\"
        $BaseName = "$($_.BaseName.Trim())"

        # Define the mappings for search/replace
        $replaceMap = @{
            "h265" = @("h.264", "x.264", "h264", "x264", "x265", "xvid", "hevc", "avc", "av1", "mpeg2")
            ""     = @("raw-hd")
        }

        # Iterate over the replacement map and apply replacements
        foreach ($newValue in $replaceMap.Keys)
        {
            foreach ($oldValue in $replaceMap[$newValue])
            {
                $BaseName = $BaseName.Replace($oldValue, $newValue, $true, [CultureInfo]::InvariantCulture)
            }
        }
    
        # Replace two or more consecutive spaces in $BaseName with a single space.
        if (([System.Text.RegularExpressions.Regex]::Match($BaseName, "\s{2,}")).Success -eq $true)
        {
            $BaseName = [System.Text.RegularExpressions.Regex]::Replace($BaseName, "\s{2,}", " ");
        }

        $full_processed_path = $BasePath.Replace($media_path, $processed_path);

        try 
        {
            $ext = $_.Extension -ieq ".mkv" ? ".mkv" : ".mp4";

            if ( $IsReprocess )
            {
                [int]$ProcessCount = 0;
                if ( [int]::TryParse($BaseName[$BaseName.Length - 1], [ref]$ProcessCount ))
                {
                    $BaseName = $BaseName.SubString(0, $BaseName.Length - 1).Trim();
                }
                $new_file = "$($processing_temp)$($BaseName) $($ProcessCount +1)$($ext)";
            }
            else 
            {
                $new_file = "$($processing_temp)$($BaseName) - proc$($ext)";  
            }      

            $current_file = $($_.Name);
            $start = Get-Date;

            # W = WRITE, L = LOCKED, NF = NOT FOUND
            $fileStatus = (CheckFileStatus -filePath $_.FullName);
            Write-Log -Message "$($fileStatus) $($current_file)" -Target File;

            $width = [console]::BufferWidth;
            Write-Host "$(" " * $width)`r" -NoNewline;
      
            if ("$($fileStatus) $($current_file)".Length -gt $width)
            {
                Write-Host "$("$($fileStatus) $($current_file)".SubString(0, $width - 1))`r" -NoNewline;
            }
            else 
            {
                Write-Host "$($fileStatus) $($current_file)`r" -NoNewline;
            }

            if ($fileStatus -in "[L]", "[NF]", "[E]")
            {
                return;
            }
        }
        catch 
        {
            Write-Log -Message "$($_)`r`nStackTrace: $($Error[0].Exception.StackTrace)" -Level Error;
        }
    
        $metadata = ((&"$($script:ffmpeg_path)\ffprobe.exe" $_.FullName -hide_banner -analyzeduration 4GB -probesize 4GB 2>&1 | Out-String ).Trim());
        $vid_stream_info = ((&"$($script:ffmpeg_path)\ffprobe.exe" $_.FullName -hide_banner -analyzeduration 4GB -probesize 4GB -v quiet -select_streams v:0 -show_streams | Out-String ).Trim());
        $aud_stream_info = ((&"$($script:ffmpeg_path)\ffprobe.exe" $_.FullName -hide_banner -analyzeduration 4GB -probesize 4GB -v quiet -select_streams a -show_streams | Out-String ).Trim());
        $sub_stream_info = ((&"$($script:ffmpeg_path)\ffprobe.exe" $_.FullName -hide_banner -analyzeduration 4GB -probesize 4GB -v quiet -select_streams s -show_streams | Out-String ).Trim());
        $vid_stream_format = ((&"$($script:ffmpeg_path)\ffprobe.exe" $_.FullName -hide_banner -analyzeduration 4GB -probesize 4GB -v quiet -select_streams v:0 -show_format -sexagesimal | Out-String ).Trim());

        $defaultAudStream = "";
        $defaultSubStream = "";

        if ( $vid_stream_info )
        {
            $vid_streams = $vid_stream_info | ParseStreamInfo -streamType "video";
        }

        if ( $aud_stream_info )
        {
            $aud_streams = $aud_stream_info | ParseStreamInfo -streamType "audio";
            $engAudStream = $aud_streams | Where-Object { ( $_.Language -ieq 'eng' ) -or ( $_.Title -ilike 'eng*' ) } | Select-Object -First 1;
    
            # If there's an English audio stream and it's not set as the default audio stream, set it now.
            if ( $engAudStream -and ( $engAudStream -is [PSCustomObject] ) -and ( $engAudStream.DispositionDefault -ne $engAudStream.Index ))
            {
                $defaultAudStream = "-disposition:a:$($engAudStream.Index) default"; 
                #Write-Log -Message "Set default audio stream: '$($defaultAudStream)'" -Level Information;
            }
        }

        if ( $sub_stream_info )
        {
            $sub_streams = $sub_stream_info | ParseStreamInfo -streamType "subtitle";
            $engSubStream = $sub_streams | Where-Object { ( $_.Language -ieq 'eng' ) -or ( $_.Title -ilike 'eng*' ) } | Select-Object -First 1;  
    
            # If there's an English sub stream and it's not set as the default sub stream, set it now.
            if ( $engSubStream -and ( $engSubStream -is [PSCustomObject] ) -and ( $engSubStream.DispositionDefault -ne $engSubStream.Index )) 
            {
                $defaultSubStream = "-disposition:s:$($engSubStream.Index) default"; 
                #Write-Log -Message "Set default sub stream: '$($defaultSubStream)'" -Level Information;
            }
        }

        # Default to all. 
        # Only select a language specific stream if a stream exists for the language.
        $AudioMap = (( $AudioLang -ieq "nomap" ) -or ( $AudioLang -ieq "none" )) ? "" : "-map $((( $AudioLang -ine "all" ) -and ( $aud_streams | Where-Object { $_.Language -ieq "$($AudioLang)" } )) ? "0:a:m:language:$($AudioLang)" : "0:a")"; 
        $SubMap = (( $SubLang -ieq "nomap" ) -or ( $SubLang -ieq "none" )) ? "" : "-map $((( $SubLang   -ine "all" ) -and ( $sub_streams | Where-OBject { $_.Language -ieq "$($SubLang)"   } )) ? "0:s:m:language:$($SubLang)"  : "0:s")";

        $ttl_duration = New-TimeSpan;
        $fps = 30;
        try
        {
            if ( -not (( $vid_streams ) -and ( [timespan]::TryParse(( $vid_streams | Select-Object -First 1 ).Duration, [ref]$ttl_duration )))) 
            {
                if ( $vid_stream_format -Match "(duration=)(.+)" )
                {
                    if ( [TimeSpan]::TryParse( $Matches[2], [ref]$ttl_duration ))
                    {
                        $Matches.Clear();
                    }
                }
            }

            if ( $metadata -Match "(\d+\.?\d*)( fps)" )
            {
                if ( [decimal]::TryParse( $Matches[1], [ref]$fps ))
                {
                    $fps = ( $fps -lt 30 ) ? 30 : [math]::Ceiling( $fps );
                    $Matches.Clear();
                }
            }
     
            if ( -not $ttl_duration )
            {
                Write-Debug "ttl_duration zero"
                # ttl_duration = [TimeSpan]( &"$($script:ffmpeg_path)\ffprobe.exe"    $_.FullName   -v error -hide_banner -analyzeduration 4GB -probesize 4GB -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 -sexagesimal | Out-String );      
                if ( [TimeSpan]::TryParse((  &"$($script:ffmpeg_path)\ffprobe.exe" "$($_.FullName)" -v error -hide_banner -analyzeduration 4GB -probesize 4GB -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 -sexagesimal | Out-String), [ref]$ttl_duration ))
                {
                    $Matches.Clear();
                }
                else 
                {
                    if (-not (($metadata -Match "(Duration: )([0-9\:\.]+)(,)" ) -and ( [TimeSpan]::TryParse( $Matches[2], [ref]$ttl_duration ))))
                    {
                        $Matches.Clear();
                        # Something might be up with the file if we're unable to get duration.
                        # Fall back to using frames for progress when parsing metadata fails.
                        Write-Host
                        Write-Log -Message "Unable to parse Total Duration." -Level Error;
                        Write-Log -Message "Inspect the file, it may be missing or end prematurely." -Level Error;
                        return; 
                    }
                }

                if ( $ShowOutputCmd )
                {
                    Write-Host
                    Write-Log -Message "$($script:ffmpeg_path)\ffprobe.exe '$($_.FullName)' -v error -hide_banner -analyzeduration 4GB -probesize 4GB -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 -sexagesimal";
                    Write-Host
                }
            }

            $itdm = ( [math]::Floor( $ttl_duration.TotalMinutes ));   
            $ismb = ( [int]( $_.Length / 1MB ));
            $impm = ( [math]::Ceiling( $ismb / $itdm ));

            if 
            (
                ((-not $Path.Contains( $movies_path )) -and ( $ismb -le 350 )) -or
                (( $Path.Contains( $movies_path )) -and ( $ismb -le 900 )) -and
                (-not $ForceConvert)
            )
            {
                # It's not worth attempting to compress.
                return;
            }
        }
        catch [Exception]
        {
            #   Write-Host "An error occurred while reading properties from the source file.";
            #   Write-Host "Skipping:" $srcFullName;
            #   Write-Error $_;
            return;  
        }   

        #$CQPRateCtrl = ($CQPRateControl -ne 0) ? $CQPRateControl : 28;
        #$BitrateCtrl = ($BitrateControl -ne "") ? $BitrateControl : "";
        
        $SrcResW = 0;
        $SrcResH = 0;
        $SrcIsResMatched = $false;
        $SrcResType = [ResTypes]::UNKNOWN;

        # if $vid_streams is null or Width fails to parse, fail over to $vid_stream_info
        if (( -not ( $vid_streams )) -or ( -not ( [int]::TryParse( ($vid_streams | Select-Object -First 1).Width, [ref]$SrcResW ))))
        {
            if ( $vid_stream_info -Match "(?m)(^width=)(\d*\r?\n?)" )
            {
                if ( [int]::TryParse( $Matches[2], [ref]$SrcResW ))
                {
                    $Matches.Clear();
                }
            }
        }

        # if $vid_streams is null or Height fails to parse, fail over to $vid_stream_info
        if (( -not ( $vid_streams )) -or ( -not ( [int]::TryParse( ($vid_streams | Select-Object -First 1).Height, [ref]$SrcResH ))))
        {
            if ( $vid_stream_info -Match "(?m)(^height=)(\d*\n?\n?)" )
            {
                if ( [int]::TryParse( $Matches[2], [ref]$SrcResH ))
                {
                    $Matches.Clear();
                }
            }
        }

        if (( $SrcResW -gt 0 ) -and ( $SrcResH -gt 0 ))
        {
            $SrcIsResMatched = $true;      
        }
        else 
        {
            $SrcRes = ( &"$($script:ffmpeg_path)\ffprobe.exe" "$($_.FullName)" -v quiet -hide_banner -analyzeduration 4GB -probesize 4GB -select_streams v:0 -show_entries stream=width, height -of csv=s=x:p=0 | Out-String ).Trim();
            $SrcIsResMatched = $SrcRes -Match '(?<width>\d+)x(?<height>\d+)';

            if ( $SrcIsResMatched -eq $true )
            {
                $SrcResW = [int]$Matches.width;
                $SrcResH = [int]$Matches.height;
            }
    
            if ( $ShowOutputCmd )
            {
                Write-Host
                Write-Log -Message "$($script:ffmpeg_path)\ffprobe.exe '$($_.FullName)' -v quiet -hide_banner -analyzeduration 4GB -probesize 4GB -select_streams v:0 -show_entries stream=width,height -of csv=s=x:p=0";
                Write-Host
            }
        }

        if ( $SrcIsResMatched )
        {
            try
            {
                $SrcAspectRatio = [Math]::Round($SrcResW / $SrcResH, 2);
            }
            catch 
            {
                Write-Log -Message "Src Width: $($SrcResW), and Src Height: $($SrcResH), must be greater than zero.";
            }

            $SrcResType = GetResType $SrcResW $SrcResH;

            if ( -not $DestIsResMatched )
            {
                $DestResType = $SrcResType;
            }
        }

        if (( -not $ForceConvert ) -and ( -not $ForceResize ))
        {
            if ( $SrcIsResMatched -eq $true )
            {
                if ( $SrcResType -eq [ResTypes]::SD )
                {
                    $tmpm = 5 
                }
                elseif ( $SrcResType -eq [ResTypes]::DVD )
                { 
                    $tmpm = 7
                }
                elseif ( $SrcResType -eq [ResTypes]::HD )
                {
                    $tmpm = 15
                }
                elseif ( $SrcResType -eq [ResTypes]::FHD )
                {
                    $tmpm = 20
                }
                elseif ( $SrcResType -eq [ResTypes]::QHD )
                {
                    $tmpm = 30
                }
                elseif ( $SrcResType -eq [ResTypes]::UHD_4K )
                {
                    $tmpm = 40
                }

                if ( $impm -le $tmpm )
                {
                    Write-Log -Message "Skipping, threshold not met: $($impm) is less than or equal to $($tmpm), size: $($ismb)MB, time: $($itdm)min" -Target File;
                    return;
                }

                if 
                ( 
                    ( $Path.Contains( $movies_path )) -and
                    (
                        (( $SrcResType -eq [ResTypes]::SD ) -and ( ($ismb * 1MB) -lt 0.8GB )) -or 
                        (( $SrcResType -eq [ResTypes]::HD ) -and ( ($ismb * 1MB) -lt 1.5GB )) -or 
                        (( $SrcResType -eq [ResTypes]::HD ) -and ( ($ismb * 1MB) -lt 3.5GB )) -or 
                        (( $SrcResType -eq [ResTypes]::FHD ) -and ( ($ismb * 1MB) -lt 5GB )) -or 
                        (( $SrcResType -eq [ResTypes]::UHD_4K ) -and ( ($ismb * 1MB) -lt 9GB ))
                    )
                )
                { 
                    return;
                }
            }
            else
            {
                if ( $impm -le 27 )
                {
                    return;
                }
            }
        }

        # $CQPRateCtrl = GetCQPRateControl;
        $msg = "`r`nCQP: I$([int]$CQPRateControl):P$([int]$CQPRateControl +1):B$([int]$CQPRateControl +2), Src: $($SrcResType), Res: $($SrcResW):$($SrcResH), iMB: $($ismb), iMB/min: $($impm), tMB/min: $($tmpm)"
        Write-Host "$($msg)`r" -NoNewline;
        Write-Log -Message $msg -Target File;
        # $BitrateCtrl = GetBitrateControl;    
    
        $svcdn = "N/A";
        $svdp = "main";
        $svbd = "8";

        if ( $vid_streams )
        {
            # Source video codec name
            $svcdn = ($vid_streams | Select-Object -First 1).CodecName;
    
            # Source video profile
            $svdp = ((($vid_streams | Select-Object -First 1).Profile) -replace " ", "").ToLower();
    
            # Source video bit depth
            $svbd = ($vid_streams | Select-Object -First 1).BitsPerRawSample -or 
            ($vid_streams | Select-Object -First 1).PixFmt.Contains( "10" );
        }

        if ( $DestIsResMatched -and $SrcIsResMatched )
        {      
            $DestAspectRatio = [math]::Round($DestResW / $DestResH, 2);

            # Don't attempt to resize if $Src and $Dest ResTypes are the same, unless ForceConvert $true.
            if 
            (
                ( $SrcResType -eq $DestResType ) -or
                (
                    ((( $SrcResW * $SrcResH ) -ge ( $DestResW * $DestResH )) -and ( -not $CanScaleDown )) -or
                    ((( $SrcResW * $SrcResH ) -le ( $DestResW * $DestResH )) -and ( -not $CanScaleUp ))
                ) -and 
                ( -not $ForceConvert )
            )
            {
                return;
            }

            if (( $SrcAspectRatio -ge 1 ) -and ( $DestAspectRatio -ge 1 )) 
            {
                $ScaleW = $DestResW;
                $ScaleH = $DestResH;
           
                # -vf scale=$w:$h
                if ( $RetainAspect )
                {
                    # Default: Constrain width.
                    $ScaleH = -1;

                    if ( $DestAspectRatio -gt $SrcAspectRatio )
                    {
                        # Constrain height.
                        $ScaleH = $DestResH;
                        $ScaleW = -1;
                    }
                }
                $ResizeRes = "-vf scale=$($ScaleW):$($ScaleH)";
                Write-Host "`r`nResizing: $($SrcResW):$($SrcResH) -> $($ScaleW -eq -1 ? [math]::Floor($DestResH * $SrcAspectRatio) : $ScaleW):$($ScaleH -eq -1 ? [math]::Floor($DestResW / $SrcAspectRatio) : $ScaleH)";
            }
            else
            {
                Write-Host
                if ( $ForceResize )
                {
                    $ResizeRes = "-vf scale=$($DestResW):$($DestResH)";
                    Write-Host "Forcing resize: $($SrcResW)x$($SrcResH) -> $($DestResW)x$($DestResH)";
                } 
                else 
                {
                    Write-Host
                    Write-Log -Message "Video will not resize: Incompatible aspect ratio. $($SrcResW)x$($SrcResH) -> $($DestResW)x$($DestResH)" -Level Error;
                    Write-Log -Message "Use `$ForceResize `$true to resize anyway." -Level Error;
                }
            }
        }
        else 
        {
            if ( ($ForceResize) -and ($DestIsResMatched) )
            {
                Write-Host
                Write-Log -Message "Forcing resize: $($SrcResW)x$($SrcResH) -> $($DestResW)x$($DestResH)";
                $ResizeRes = "-vf scale=$($DestResW):$($DestResH)";
            }  
            else
            {
                if (( $DestIsResMatched ) -and ( -not $SrcIsResMatched ))
                {
                    Write-Log -Message "Unable to parse current resolution. Use -ForceResize `$true to resize.";
                }
            }
        }

        $dacdn = "copy";
        if ( $aud_streams )
        {
            $sacdn = ($aud_streams | Select-Object -First 1).CodecName;
        }
        else
        {
            $sacdn = ( &"$($script:ffmpeg_path)\ffprobe.exe" $_.FullName -v quiet -hide_banner -analyzeduration 4GB -probesize 4GB -select_streams a:0 -show_entries stream=codec_name -of default=noprint_wrappers=1:nokey=1 | Out-String ).Trim();
        }
        if ( $sacdn -ieq "wmapro" )
        {
            $dacdn = "eac3"
        }

        $ffmpeg_args = "-hide_banner -y -threads 0 -hwaccel cuvid -hwaccel_output_format cuda -copy_unknown -analyzeduration 4GB -probesize 4GB "; # -f (($_.Name -Match "AV1|xvid") ? "auto" : "cuvid");
        $ffmpeg_args += '-i "{0}" {1} -default_mode infer_no_subs -map 0:v:0 {2} {3} -dn -map_metadata 0 -map_chapters 0 -c:v {4} -c:a {7} -c:s copy {5} {8} {9} "{6}"' -f $_.FullName, $ResizeRes, "$($AudioMap)", "$($SubMap)", "{0}", "{1}", "$($new_file)", "$($dacdn)", "$($defaultAudStream)", "$($defaultSubStream)";
    
        if ( $dvcdn -ieq "hevc" )
        {
            $10bitProfile = "";
            $10bitRawSample = $false;
            $10bitPixel = "";
            $HDR = "";
      
            if ( $vid_streams )
            {
                $stream = ( $vid_streams | Select-Object -First 1 );

                if ( $stream.Profile.Contains( "10" ))
                {
                    $10bitProfile = "-profile:v main10 ";
                    Write-Log -Message "Detected: 10-bit profile";
                }

                if ( $stream.BitsPerRawSample -eq "10" )
                {
                    $10bitRawSample = $true;
                    Write-Log -Message "Detected: 10-bit raw sample";
                }
        
                if ( $10bitRawSample -or $stream.PixFmt.Contains( "10" ))
                {
                    $10bitPixel = "-pix_fmt p010le ";
                    Write-Log -Message "Detected: 10-bit pixel format";
                }
      
                if (( $10bitProfile -and ( $10bitPixel -or $10bitRawSample )) -and
                    ( $stream.ColorPrimaries -ilike "bt2020*" ) -and 
                    ( $stream.ColorSpace -ilike "bt2020*" ) -and
                    ( $stream.ColorTransfer -ieq "pq" -or $stream.ColorTransfer -ieq "hlg" -or $stream.ColorTransfer -ieq "smpte2084" ))
                {
                    $HDR = "-color_primaries $($stream.ColorPrimaries) -color_trc $($stream.ColorTransfer) -colorspace $($stream.ColorSpace) ";
                    Write-Log -Message "Detected: HDR";
                }
            }
      
            $hevc_nvenc_args = "$($ffmpeg_args)" -f "hevc_nvenc", "-bufsize:v 5MB $($10bitProfile)$($10bitPixel)$($HDR)-preset:v p4 -tune:v hq -tier:v main -rc:v constqp -init_qpI:v $([int]$CQPRateControl) -init_qpP:v $([int]$CQPRateControl +1) -init_qpB:v $([int]$CQPRateControl +2) -rc-lookahead:v 32 -spatial-aq:v 1 -aq-strength:v 8 -temporal-aq:v 1 -b_ref_mode:v 1";
        }
        else
        {
            Write-Host
            Write-Log -Message "No profile found for $($dvcdn).";
            Exit;
        }

        # Test ffmpeg command
        $test_args = $hevc_nvenc_args.Replace( '"' + $new_file + '"', "-ss 0 -to 10 -f null NUL" );

        $maxRetries = 3
        $retryCount = 1
        $exitCode = 1

        # Retry the process until the maximum number of retries is reached or the exit code is 0 (success)
        while (( $retryCount -le $maxRetries ) -and ( $exitCode -ne 0 ))
        {
            if ( -not $test )
            {
                $test = New-Object System.Diagnostics.Process
            }

            $test.StartInfo.FileName = "$($script:ffmpeg_path)\ffmpeg.exe"
            $test.StartInfo.Arguments = "$($test_args)"
            $test.StartInfo.UseShellExecute = $false
            $test.StartInfo.RedirectStandardError = $true
            $test.StartInfo.RedirectStandardOutput = $false
            $test.StartInfo.CreateNoWindow = $true

            Write-Log -Message "Test Command: $($script:ffmpeg_path)\ffmpeg.exe $($test_args)" -Target File;

            $test.Start() | Out-Null

            while ( -not $test.HasExited )
            {
                if ( $test.StandardError.Peek() )
                {
                    $line = $test.StandardError.ReadLineAsync().Result;
                    $line | Out-Null; 
                }
            }

            $test.WaitForExit();
            $exitCode = $test.ExitCode;

            if ( $exitCode -ne 0 )
            {
                $errorOutput = $test.StandardError.ReadToEnd()
                Write-Log -Message "Test Command Failed. Retry attempt: $($retryCount). Error: $($errorOutput)";

                if (( $retryCount -eq 1 ) -and ( $test_args.Contains( "-hwaccel cuvid" )))
                {
                    # Replacing '-hwaccel cuvid' with '-hwaccel auto' to handle cases where 'cuvid' is not supported
                    $test_args = $test_args.Replace("-hwaccel cuvid", "-hwaccel auto");
                    $hevc_nvenc_args = $hevc_nvenc_args.Replace("-hwaccel cuvid", "-hwaccel auto");
                }

                if (( $retryCount -eq 2 ) -and ( $test_args.Contains( "-hwaccel_output_format cuda" )))
                {
                    $test_args = $test_args.Replace("-hwaccel_output_format cuda", "");
                    $hevc_nvenc_args = $hevc_nvenc_args.Replace("-hwaccel_output_format cuda", "");
                }
        
                if (( $retryCount -eq 3 ) -and ( $test_args.Contains( "-hwaccel auto" )))
                {
                    $test_args = $test_args.Replace("-hwaccel auto", "");
                    $hevc_nvenc_args = $hevc_nvenc_args.Replace("-hwaccel auto", "");
                }
                $backoff = [math]::Pow(2, $retryCount)
                Write-Log -Message "Retrying in $backoff seconds..."
                Start-Sleep -Seconds $backoff;
            }
    
            $retryCount++;
        }

        if ( $exitCode -ne 0 )
        {
            Write-Log "Exit Code: $exitCode"
            Write-Log -Message "Test Command failed after $maxRetries attempts.";
            Write-Log -Message "$($script:ffmpeg_path)\ffmpeg.exe $($test_args)";
            return
        }

        if ( $ShowOutputCmd )
        {
            Write-Host
            Write-Log -Message "Run  Command: $($script:ffmpeg_path)\ffmpeg.exe $($hevc_nvenc_args)" -Target Host;
            Write-Host
        }

        if (( $ShowProgress ) -and (( $ttl_duration.Ticks / 100 ) -gt 0 ))
        {
            $process = New-Object System.Diagnostics.Process;
            try
            {
                Write-Log -Message "Run  Command: $($script:ffmpeg_path)\ffmpeg.exe $($hevc_nvenc_args)" -Target File;

                $process.StartInfo.Filename = "$($script:ffmpeg_path)\ffmpeg.exe";
                $process.StartInfo.Arguments = "$($hevc_nvenc_args)";
                $process.StartInfo.UseShellExecute = $false;
                $process.StartInfo.RedirectStandardError = $true;
                $process.StartInfo.RedirectStandardOutput = $false;
                $process.StartInfo.CreateNoWindow = $true;        
        
                $process.Start() > $null;
          
                $activity = "$($svcdn) $($SrcResType) -> $($dvcdn) $($DestResType)";
                $status = "{0:p2} $($_.Name)" -f 0;      
                $previous_progress = 0;

                $speed = ", 0.00x".PadRight(7, " ");

                Write-Progress -Activity ($activity + $speed) -Status "$($status)";

                $current_duration = New-TimeSpan;

                while ( -not $process.HasExited )
                {
                    if ( $process.StandardError.Peek() )
                    {
                        $current_line = $process.StandardError.ReadLineAsync().Result;            
                        if ( $current_line )
                        {
                            $prev_info = $current_info;
                            $current_info = $current_line.Trim();
              
                            if ( $current_info -Match "\[.+\@.+\].*" )
                            {
                                $errors += "$($current_info)`r`n";
                            }
              
                            if (( $current_info -Match "^error.*audio.*" ) -or ( $current_info -ieq "Error while decoding stream #0:0: Invalid data found when processing input" ))
                            {  
                                # Don't throw on audio errors or invalid input.
                            }
                            elseif ( $current_info -Match "^error.*" )
                            {
                                throw;
                            }              

                            # Get the time stamp from the ffmpeg output. HH:mm:ss.nnn                
                            $IsTimeMatched = $current_info -Match "(time=)(\d{2}:\d{2}:\d{2}\.\d{2,3})";

                            # If the duration is found.
                            if (( $IsTimeMatched ) -and ( [TimeSpan]::TryParse( $Matches[2], [ref]$current_duration ) )) 
                            {
                                $Matches.Clear();
                                # Divide current_duration.Ticks by ttl_duration.Ticks / 100.                

                                $progress = $current_duration.Ticks / ( $ttl_duration.Ticks / 100 )

                                if ( $progress -gt $previous_progress )
                                {
                                    $percent_complete = "{0:p2}" -f ( $progress / 100 );
                                    $status = "$($percent_complete) $($_.Name)"; 

                                    if ( $LogVerbose )
                                    {
                                        Write-Log -Message "$($current_info) $($percent_complete)" -Target File;
                                    }
                                    else 
                                    {
                                        if 
                                        ( 
                                            -not ( $current_info -Match "(frame=)" )
                                        )
                                        {
                                            Write-Log -Message "$($current_info) $($percent_complete)" -Target File;
                                        }
                                    }

                                    if ( $current_info -Match "(speed=)(\W*\d*)(\.*)(\d*)(x)" )
                                    {
                                        $speed = (", " + 
                                            "$($Matches[2].Trim())" + 
                                            "$(($Matches[3].Trim() -eq `"`") ? `".`" : $Matches[3].Trim())" + 
                                            "$(($Matches[4].Trim() -eq `"`") ? `"`"  : $Matches[4].Trim())".PadRight(2, "0").Substring(0, 2) +
                                            "$($Matches[5].Trim())").PadRight(7, " ");
                                        $Matches.Clear();
                                    }

                                    Write-Progress -Activity ($activity + $speed) -Status $status -PercentComplete $progress;
                                    $previous_progress = $progress;
                                }
                            }
                            else 
                            {
                                # Don't log repeated warnings.
                                if ( $LogVerbose )
                                {
                                    Write-Log -Message "$($current_info)" -Target File;
                                }
                                else 
                                {
                                    if 
                                    ( 
                                        -not ( $current_info -Match "(frame=)" )
                                    )
                                    {
                                        Write-Log -Message "$($current_info)" -Target File;
                                    }
                                }
                            }
                        }
                    }

                    # if (([System.Console]::ReadKey()).Key -eq "Q" )
                    # {
                    #   $process.Kill();
                    #   $LASTEXITCODE = -1;
                    #   Return;
                    # }
                }        
        
                $process.WaitForExit();
                $stop = Get-Date;

                Write-Log -Message "End Process" -Target File;
            }
            catch
            {
                Write-Host
                Write-Error "`r`n$($errors)$($prev_info)`r`n$($current_info)";
                return;
            }
            finally
            {
                Write-Progress -Activity $activity -Completed;
                if ( -not $process.HasExited )
                {        
                    $process.Kill();
                }
                else
                {
                    # LASTEXITCODE: -1 == Aborted
                    if ( $LASTEXITCODE -ne -1 )
                    {            
                        if ( $LASTEXITCODE -eq 0 )
                        {
                            $new_file_length = (Get-ChildItem -LiteralPath $new_file).Length;
              
                            # If the processed file is greater than 100KB
                            if ( $new_file_length -gt 100KB )
                            {
                                $new_vid_stream_info = ((&"$($script:ffmpeg_path)\ffprobe.exe" $($new_file) -v quiet -hide_banner -analyzeduration 4GB -probesize 4GB -select_streams v:0 -show_streams | Out-String ).Trim());

                                if ( $new_vid_stream_info )
                                {
                                    $new_vid_streams = $new_vid_stream_info | ParseStreamInfo -streamType "video";
                                }
                
                                $dvcdn = "N/A";
                                $dvdp = "main";
                                $dvbd = "8";
                
                                if ( $new_vid_streams )
                                {
                                    # New video codec name
                                    $dvcdn = ($new_vid_streams | Select-Object -First 1).CodecName;
                
                                    # New video profile
                                    $dvdp = ((($new_vid_streams | Select-Object -First 1).Profile) -replace " ", "").ToLower(); ;
                
                                    # New video bit depth
                                    $dvbd = ($new_vid_streams | Select-Object -First 1).BitsPerRawSample -or
                                    ($new_vid_streams | Select-Object -First 1).PixFmt.Contains( "10" );
                                }

                                try
                                {
                                    $osmb = [int]( $new_file_length / 1MB );
                                    $ompm = [math]::Ceiling( $osmb / $itdm );
                  
                                    ShowSummary $itdm $ismb $osmb $impm $ompm $svcdn $dvcdn $svdp $dvdp $svbd $dvbd $sacdn $dacdn;
                                }
                                catch [Exception]
                                {
                                }
              
                                if (( $ismb -le $osmb ) -and # If the input size in MB is less than the output size in MB.
                                    ( $ResizeRes -eq "" ) -and # And the file was not scaled up. (If it was, the output will almost certainly be larger.)
                                    ( -not $ForceConvert )) # And ForceConvert not enabled.
                                {
                                    Write-Host
                                    Write-Error "Output file is larger than the original.";
                                    Write-Host
                                    Write-Host "Removing: $($new_file)";
                                    $try = 0;
                                    do
                                    {
                                        $try += 1;
                                        Remove-WithRetry -LiteralPath $new_file;

                                        if ( Test-Path -LiteralPath $new_file -ErrorAction SilentlyContinue )
                                        {                    
                                            if ( $try -gt 5 )
                                            {
                                                Write-Host
                                                Write-Log -Message "An error occurred while removing file: $($new_file). Skipping." -Level Error;
                                                break;
                                            }
                                            Write-Host
                                            Write-Log -Message "An error occurred while removing file: $($new_file). ($($try)/5) Retrying in 15 seconds." -Level Error;
                                            Start-Sleep -Seconds 15;
                                        }
                                    } while ( Test-Path -LiteralPath $new_file )
                  
                                    # Don't rename the file if MoveOnCompletion is False. This might be a test.
                                    if ( $MoveOnCompletion )
                                    {
                                        $fn = ( $IsReprocess ) ? "$($_.BaseName.Trim())$($_.Extension)" : "$($_.BaseName.Trim()) - proc$($_.Extension)";
                                        Rename-WithRetry -LiteralPath $_.FullName -NewName $fn;
                                    }

                                    if ( $RefreshArrOnCompletion )
                                    {                    
                                        $titleIds = GetArrTitleIds -type $type -dirInfo $_.Directory -baseUri $baseUri -apiKey $apiKey;
                    
                                        foreach ( $id in $titleIds ) 
                                        { 
                                            RefreshArrTitles -type $type -titleIds $id -baseUri $baseUri -apiKey $apiKey;
                                        }
                                    }
                                }
                                else
                                {
                                    if ( $MoveOnCompletion )
                                    {
                                        # If the destination path doesn't exist, create it.
                                        if ( -not ( Test-Path -LiteralPath $full_processed_path ))
                                        {
                                            New-Item -Path $full_processed_path -ItemType Directory > $null
                                        }
                                        # Move the new file from the working path to the destination path.
                                        Move-WithRetry -LiteralPath "$($new_file)" -Destination "$($_.Directory)" -Force;
                                        # Move the original file to the processed path.
                                        Move-WithRetry -LiteralPath $_.FullName -Destination "$($full_processed_path)" -Force;
                                        Write-Host "Completed.";
                                    }

                                    if ( $RefreshArrOnCompletion )
                                    {
                                        $titleIds = GetArrTitleIds -type $type -dirInfo $_.Directory -baseUri $baseUri -apiKey $apiKey;
                    
                                        foreach ( $id in $titleIds) 
                                        { 
                                            RefreshArrTitles -type $type -titleIds $id -baseUri $baseUri -apiKey $apiKey;
                                        }
                                    }
                                }
                            }
                            else
                            {
                                if ( $errors -eq "" )
                                {
                                    Write-Host
                                    Write-Error "Destination file is too small.";
                                }
                                if ( Test-Path -LiteralPath $new_file )
                                {
                                    Write-Host
                                    Write-Host "Removing: $($new_file)";
                                    Remove-WithRetry -LiteralPath $new_file;
                                }
                
                                if ( $ExitOnError )
                                {
                                    Exit;
                                }
                            }
                        } 
                        else
                        {
                            Write-Host 
                            Write-Log -Message "ExitCode indicates failure!" -Level Error;
                            Write-Log -Message "FFmpeg encountered an error with exit code $($LASTEXITCODE)" -Level Error;
                            if ( $ExitOnError )
                            {
                                Exit;
                            }         
                        }
                    }
                }
            }
        }
        else
        {
            if ( ($ShowProgress) -and (($ttl_duration.Ticks / 100) -eq 0) )
            {
                Write-Host
                Write-Error "Unable to ShowProgress when Total Duration is zero."
                if ( $ExitOnError )
                {
                    Exit;
                }
            }
            Write-Host 
            Start-Sleep -Seconds 5;
      
            try
            {
                Invoke-Expression "$($script:ffmpeg_path)\ffmpeg.exe $($hevc_nvenc_args)";
            }
            finally
            {
                $stop = Get-Date;
            }
      
            if ( $LASTEXITCODE -ne -1 )              
            {
                if ( $LASTEXITCODE -eq 0 )
                {
                    $new_file_length = (Get-ChildItem -LiteralPath $new_file).Length;

                    # If the processed file is greater than 100KB
                    if ( $new_file_length -gt 100KB )
                    {
                        $new_vid_stream_info = ((&"$($script:ffmpeg_path)\ffprobe.exe" $($new_file) -v quiet -hide_banner -analyzeduration 4GB -probesize 4GB -select_streams v:0 -show_streams | Out-String ).Trim());

                        if ( $new_vid_stream_info )
                        {
                            $new_vid_streams = $new_vid_stream_info | ParseStreamInfo -streamType "video";
                        }
                
                        $dvcdn = "N/A";
                        $dvdp = "main";
                        $dvbd = "8";
            
                        if ( $new_vid_streams )
                        {
                            # New video codec name
                            $dvcdn = ($new_vid_streams | Select-Object -First 1).CodecName;
            
                            # New video profile
                            $dvdp = ((($new_vid_streams | Select-Object -First 1).Profile) -replace " ", "").ToLower(); ;
            
                            # New video bit depth
                            $dvbd = ($new_vid_streams | Select-Object -First 1).BitsPerRawSample -or
                            ($new_vid_streams | Select-Object -First 1).PixFmt.Contains( "10" );
                        }

                        try
                        {
                            $osmb = [int]( $new_file_length / 1MB );
                            $ompm = [math]::Ceiling( $osmb / $itdm );
          
                            ShowSummary $itdm $ismb $osmb $impm $ompm $svcdn $dvcdn $svdp $dvdp $svbd $dvbd $dacdn $dacdn;
                        }
                        catch [Exception]
                        {
                            Write-Log -Message "Error calculating summary: $($_.Exception.Message)" -Level Error
                            Write-Log -Message $_.Exception.StackTrace -Level Error
                        }             
        
                        if (( $ismb -le $osmb ) -and # If the input size in MB is less than the output size in MB.
                            ( $ResizeRes -eq "" ) -and # And the file was not scaled up. (If it was, the output will almost certainly be larger.)
                            ( -not $ForceConvert )) # And ForceConvert not enabled.
                        {
                            Write-Host
                            Write-Error "Output file is larger than the original.";
                            Write-Host
                            Write-Host "Removing: $($new_file)";
                            $try = 0;
                            do
                            {
                                $try += 1;
                                Remove-WithRetry -LiteralPath $new_file;

                                if ( Test-Path -LiteralPath $new_file -ErrorAction SilentlyContinue )
                                {                    
                                    if ( $try -gt 5 )
                                    {
                                        Write-Host
                                        Write-Log -Message "An error occurred while removing file: $($new_file). Skipping." -Level Error;
                                        break;
                                    }
                                    Write-Host
                                    Write-Log -Message "An error occurred while removing file: $($new_file). ($($try)/5) Retrying in 15 seconds." -Level Error;
                                    Start-Sleep -Seconds 15;
                                }
                            } while ( Test-Path -LiteralPath $new_file )
          
                            # Don't rename the file if MoveOnCompletion is False. This might be a test.
                            if ( $MoveOnCompletion )
                            {
                                $fn = ( $IsReprocess ) ? "$($_.BaseName.Trim())$($_.Extension)" : "$($_.BaseName.Trim()) - proc$($_.Extension)";
                                Rename-WithRetry -LiteralPath $_.FullName -NewName "$($fn)";
                            }

                            if ( $RefreshArrOnCompletion )
                            {
                                $titleIds = GetArrTitleIds -type $type -dirInfo $_.Directory -baseUri $baseUri -apiKey $apiKey;
            
                                foreach ( $id in $titleIds) 
                                { 
                                    RefreshArrTitles -type $type -titleIds $id -baseUri $baseUri -apiKey $apiKey;
                                }
                            }
                        }
                        else
                        {
                            if ( $MoveOnCompletion )
                            {
                                if ( -not ( Test-Path -LiteralPath $full_processed_path ))
                                {
                                    New-Item -Path $full_processed_path -ItemType Directory > $null
                                }
                                Move-WithRetry -LiteralPath "$($new_file)" -Destination "$($_.Directory)" -Force;
                                Move-WithRetry -LiteralPath $_.FullName -Destination "$($full_processed_path)" -Force;
                                Write-Host "Completed.";
                            }

                            if ( $RefreshArrOnCompletion )
                            {
                                $titleIds = GetArrTitleIds -type $type -dirInfo $_.Directory -baseUri $baseUri -apiKey $apiKey;
            
                                foreach ( $id in $titleIds) 
                                { 
                                    RefreshArrTitles -type $type -titleIds $id -baseUri $baseUri -apiKey $apiKey;
                                }
                            }
                        }
                    }
                    else
                    {
                        Write-Host        
                        Write-Error "Destination file is too small.";
                        Write-Host
                        Write-Host "Removing: $($new_file)";
                        if ( Test-Path -LiteralPath $new_file )
                        {
                            Write-Host
                            Write-Host "Removing: $($new_file)";
                            Remove-WithRetry -LiteralPath $new_file;
                        }

                        if ( $ExitOnError )
                        {
                            Exit;
                        }
                    }
                }
                else 
                {
                    Write-Host 
                    Write-Log -Message "ExitCode indicates failure!" -Level Error;
                    Write-Log -Message "FFmpeg encountered an error with exit code $($LASTEXITCODE)" -Level Error;
          
                    if ( $ExitOnError )
                    {
                        Exit;
                    }         
                }
            }
        }

        try
        {
            $i = New-Object System.TimeSpan(( $stop - $start ).Ticks );
            $elapsed = "{0:HH:mm:ss}" -f ( New-Object System.DateTime ( 1, 1, 1, $i.Hours, $i.Minutes, $i.Seconds ));
            Write-Host "Elapsed: $($elapsed)";
        }
        catch 
        {
            # Write-Host
            # Write-Host "Start: $($start)"
            # Write-Host "Stop: $($stop)"
        }
    }
}
catch [Exception]
{
    Write-Host
    Write-Log -Message $_ -Level Error;
    Write-Log -Message $Error[0].Exception.Message -Level Error;
    Write-Log -Message $Error[0].Exception.StackTrace -Level Error;

    Start-Sleep -Seconds 30;
}
