# Technical Debt

- Current WebUI is implemented as embedded HTML/CSS/JS in Python; acceptable for v0.1 but should be split if UI complexity grows.
- Live TQSDK behavior must be revalidated when SDK versions change.
- The current Windows workspace path contains non-ASCII characters; `uv` editable installs may write a mangled `.pth`. Run `scripts/agentic-sdlc/repair-uv-editable-path.ps1` after `uv sync` if console entry points cannot import `option_data_manager`.
