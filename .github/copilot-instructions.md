# Repository Instructions

- When changing script parameters, modes, defaults, or runtime behavior in `ffmpeg_h265.ps1`, update `README.md` in the same change.
- Keep usage examples, parameter tables, and behavior notes aligned with the script's current behavior.
- If a mode has conditional requirements, document them explicitly. Example: standalone `-ViewReport` does not require `-Path` or config discovery.
- If hash options or defaults change, update all README references, including feature bullets and command examples.
- Maintain comprehensive comment-based help in `ffmpeg_h265.ps1` with .PARAMETER sections for all parameters and .EXAMPLE sections with usage examples, aligned with README.md.
- Use compressed format for comment-based help sections (no blank lines between .PARAMETER or .EXAMPLE blocks).