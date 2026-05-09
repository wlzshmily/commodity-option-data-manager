# Task Registry

| Task ID | Status | Owner | Summary | Verification |
| --- | --- | --- | --- | --- |
| REL-001 | Done | Codex | Restore package/import/runtime metadata and CLI entrypoints | `uv run python -c "import option_data_manager"`; `uv run odm-collect --help` |
| REL-002 | Done | Codex | Initialize agentic-sdlc project governance | `scripts/agentic-sdlc/check-agentic-sdlc.ps1 -Root .` |
| REL-003 | Done | Codex | Consolidate local API, settings, API keys, and refresh endpoint | pytest API tests |
| REL-004 | Done | Codex | Preserve WebUI baseline and wire real settings/API information | WebUI TestClient smoke |
| REL-005 | Done | Codex | Add focused unit/API tests and sample database fixtures | `uv run pytest -q` |
| REL-006 | In Progress | Codex | Run bounded live TQSDK collection windows and tune batch thresholds | Background refresh worker; `odm-collect --max-batches` live reports; WebUI batch-progress read model |
| REL-007 | Done | Codex | Move local development target to WSL2 Ubuntu and align cloud Linux runtime setup | WSL setup script; Linux secret protector; pytest |
