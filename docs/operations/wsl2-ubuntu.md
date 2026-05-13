# WSL2 Ubuntu Development

This project is now configured so local development, cloud deployment, and CI can all use the same Linux-oriented workflow.

## Recommended Location

Clone or move the repository inside the WSL filesystem, for example:

```bash
mkdir -p ~/projects
cd ~/projects
git clone https://github.com/wlzshmily/commodity-option-data-manager.git
cd commodity-option-data-manager
```

Avoid running the Python virtual environment from `/mnt/c/...` for long market collection jobs. The Windows-mounted filesystem is slower for SQLite and many small files.

`scripts/setup-wsl-ubuntu.sh` refuses to run from `/mnt/c`, `/mnt/d`, or `/mnt/e` by default because creating `.venv` on a Windows-mounted path can fail with permission errors and can also overwrite the Windows virtual environment. Clone the repository into the WSL filesystem instead.

## First-Time Setup

```bash
bash scripts/setup-wsl-ubuntu.sh
```

The script checks Python 3.11+, verifies `uv`, runs `uv sync --dev`, creates runtime folders, and creates `.env` from `.env.example` when missing.
The project is pinned to Python 3.11 through `.python-version`; this avoids TQSDK/requests TLS issues observed with Ubuntu 26.04's default Python 3.14.

Install `uv` in Ubuntu when needed:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Runtime Environment

Common local defaults:

```bash
export ODM_DATABASE_PATH=data/option-data-current.sqlite3
export ODM_WEB_HOST=127.0.0.1
export ODM_WEB_PORT=8765
```

For command-line collection or cloud jobs, prefer transient credentials:

```bash
export TQSDK_ACCOUNT="your-account"
export TQSDK_PASSWORD="your-password"
uv run odm-collect --max-batches 100 --wait-cycles 1
```

Tuned full-market catch-up can use independent process shards. Each worker
opens its own TQSDK API instance and owns a disjoint underlying range:

```bash
uv run odm-collect-parallel \
  --workers 2 \
  --option-batch-size 40 \
  --wait-cycles 1 \
  --max-batches-per-worker 50 \
  --until-complete \
  --max-waves 20
```

Realtime UI freshness should use quote-only shards and keep subscriptions
alive. Keep `quote-shard-size` bounded; very large symbol lists can trigger
TQSDK server-side limits.

```bash
uv run odm-quote-stream \
  --worker-index 0 \
  --worker-count 2 \
  --quote-shard-size 1000
```

Validate credentials and network connectivity without starting collection:

```bash
uv run odm-test-tqsdk
```

For local WebUI use, credentials may also be saved through Settings. On WSL/Linux, saved secrets are encrypted with a per-user Fernet key at:

```text
~/.config/option-data-manager/secret.key
```

Set `ODM_SECRET_KEY_FILE` to place that key somewhere else. Keep the key file private and do not commit it.

New Codex sessions can reuse saved WebUI credentials as long as they run as
the same WSL/Linux user and use the same Fernet key file. A different Linux
user, a regenerated key file, or a different `ODM_SECRET_KEY_FILE` path cannot
decrypt existing `fernet:` secrets; in that case re-save the password from the
WebUI Settings page or point the runtime back to the original key file.

## Start The WebUI

```bash
uv run odm-webui
```

Open from Windows:

```text
http://127.0.0.1:8765/
```

## Local Server Control

Use the project script for routine WebUI lifecycle operations instead of
manually starting and killing processes. The script stops realtime Quote and
metrics workers before restarting the WebUI, which prevents stale worker
processes from holding the SQLite runtime database.

```bash
scripts/odm-server.sh status
scripts/odm-server.sh start
scripts/odm-server.sh stop
scripts/odm-server.sh restart
```

The script honors these environment variables when needed:

```bash
ODM_WEB_HOST=127.0.0.1
ODM_WEB_PORT=8765
ODM_DATABASE_PATH=data/option-data-current.sqlite3
```

Logs are written under `data/` by default:

```text
data/webui-8765.out.log
data/webui-8765.err.log
```

## Verification

```bash
uv run python --version  # should be Python 3.11.x
uv run pytest -q
uv run python -m compileall -q src tests option_data_manager
bash scripts/agentic-sdlc/check-agentic-sdlc.sh
```

The PowerShell SDLC checker remains available for Windows, but WSL should use the shell checker.

## Migrating From Windows Runtime Data

The Windows SQLite database can be copied for non-secret data, but DPAPI-protected credentials cannot be decrypted inside WSL. Re-enter TQSDK credentials in WSL through environment variables or the WebUI Settings page.
