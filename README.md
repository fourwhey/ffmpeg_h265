# 🎬 ffmpeg_h265

PowerShell script for batch encoding video files to H.265 using FFmpeg, with intelligent filtering and customizable sort options. Ideal for media library optimization and automated archiving.

## 🚀 Features

- Recursively process files in a given directory
- Customizable sort expression and user-defined filters
- Subtitle language mapping and exclusion support
- Optional logging for audit and error tracking
- Designed for use with `ffmpeg.exe` installed and accessible in PATH

## 📦 Prerequisites

- [FFmpeg](https://ffmpeg.org/) must be installed and accessible via command line
- Windows PowerShell (v5.1 or higher)

## 🛠️ Usage

Here’s an example to get you started:

```powershell
.\ffmpeg_h265.ps1 `
    -Path "V:\Media\Movies\" `
    -SortExpression @{ e='Name'; Descending=$false } `
    -UserFilter { $_.LastAccessTime -ge (Get-Date).AddDays( -3 )} `
    -SubLang nomap `
    -LogEnabled
