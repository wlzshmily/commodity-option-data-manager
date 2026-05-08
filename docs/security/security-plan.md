# Security Plan

- Bind local services to `127.0.0.1` by default.
- Store TQSDK password using Windows DPAPI when available.
- Mask password and API key values in API, UI, logs, and reports.
- Full API keys are shown only once at creation time.
- API Key enforcement is optional by default and can be enabled through settings.

