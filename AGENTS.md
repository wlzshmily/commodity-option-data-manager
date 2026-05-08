# Agent Operating Rules

This project follows agentic-sdlc delivery controls.

- Treat `PROJECT-CONTEXT.md`, `docs/project-status.md`, and `docs/project-index.md` as the durable project memory.
- Do not store real TQSDK passwords, raw API keys, or live credential evidence in tracked files.
- Runtime SQLite files belong under `data/` and are ignored by git.
- Development work must be traceable to approved requirements or an approved change request.
- Before marking delivery work complete, run the relevant tests and update task, risk, traceability, and handoff records.

