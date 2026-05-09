# Technical Debt

- Current WebUI is implemented as embedded HTML/CSS/JS in Python; acceptable for v0.1 but should be split if UI complexity grows.
- Live TQSDK behavior must be revalidated when SDK versions change.
- Windows workspace support remains available, but active development should happen inside the WSL2 Ubuntu filesystem to avoid `.pth`, SQLite, and file-watcher issues from `/mnt/c` paths.
