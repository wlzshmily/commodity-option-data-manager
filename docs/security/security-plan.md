# Security Plan

- Bind local services to `127.0.0.1` by default.
- Store TQSDK password using Windows DPAPI on Windows and a per-user Fernet key file on WSL/Linux.
- Keep `~/.config/option-data-manager/secret.key` or `ODM_SECRET_KEY_FILE` private and out of git.
- Mask password and API key values in API, UI, logs, and reports.
- Full API keys are shown only once at creation time.
- API Key enforcement is optional by default and can be enabled through settings.
