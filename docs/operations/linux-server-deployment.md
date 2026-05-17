# Linux Server Deployment

This runbook covers deploying Option Data Manager to a standalone Ubuntu server.
It is written for the current Linux-oriented runtime and mirrors the verified
test-server deployment completed on 2026-05-11.

## Scope

- Runtime: Ubuntu server, Python 3.11 managed by `uv`.
- Application service: `odm-webui`, which mounts the local API routes.
- Runtime database: SQLite under `/opt/option-data-manager/shared/data/`.
- Service manager: `systemd`.
- Default safe binding: `127.0.0.1:8765`.
- Optional test-only binding: `0.0.0.0:8765`.

Do not store real TQSDK passwords, raw API keys, or live credential evidence in
tracked files. Enter credentials through the WebUI settings page or an
untracked runtime environment.

## Server Layout

Use release directories so a verified build can be switched atomically:

```text
/opt/option-data-manager/
  current -> /opt/option-data-manager/releases/<short-sha>
  current-release.txt
  releases/
    <short-sha>/
  shared/
    .env
    data/
    docs-qa-live-evidence/
    sdk-contract-reports/
    uv-python/
```

Runtime data and reports live under `shared/` so a new release does not delete
SQLite state or local evidence. The application repository paths `data/`,
`docs/qa/live-evidence/`, and `docs/qa/sdk-contract-reports/` should be symlinks
to those shared directories in each release.

## First-Time Server Setup

Run as an administrator on the server:

```bash
apt-get update
apt-get install -y curl ca-certificates git rsync

if ! id -u odm >/dev/null 2>&1; then
  useradd --system --create-home --home-dir /home/odm --shell /usr/sbin/nologin odm
fi

mkdir -p /opt/option-data-manager/{releases,shared/data,shared/docs-qa-live-evidence,shared/sdk-contract-reports,shared/uv-python}
mkdir -p /home/odm/.config/option-data-manager
chmod 700 /home/odm/.config /home/odm/.config/option-data-manager
chown -R odm:odm /home/odm /opt/option-data-manager

curl -LsSf https://astral.sh/uv/install.sh | sh
```

Install Python 3.11 into a service-readable directory, not under `/root`, so the
`odm` system user can execute the virtual environment:

```bash
UV_PYTHON_INSTALL_DIR=/opt/option-data-manager/shared/uv-python \
  /root/.local/bin/uv python install 3.11
```

## Deploy A GitHub Commit

Prefer deploying an explicit GitHub commit SHA instead of an uncommitted local
worktree. Replace `SHA` with the target commit from `main`.

```bash
SHA="8e7f1c10144712054a71d28a0ed382de949290d8"
SHORT="${SHA:0:12}"
REPO_URL="https://github.com/wlzshmily/commodity-option-data-manager.git"

mkdir -p /opt/option-data-manager/git-cache/repo.git
if [ ! -d /opt/option-data-manager/git-cache/repo.git/objects ]; then
  rm -rf /opt/option-data-manager/git-cache/repo.git
  git clone --bare "$REPO_URL" /opt/option-data-manager/git-cache/repo.git
else
  git -C /opt/option-data-manager/git-cache/repo.git fetch --prune origin "+refs/heads/*:refs/heads/*"
fi

git -C /opt/option-data-manager/git-cache/repo.git cat-file -e "$SHA^{commit}"

rm -rf "/opt/option-data-manager/releases/$SHORT"
mkdir -p "/opt/option-data-manager/releases/$SHORT"
git -C /opt/option-data-manager/git-cache/repo.git archive "$SHA" \
  | tar -C "/opt/option-data-manager/releases/$SHORT" -xf -

cd "/opt/option-data-manager/releases/$SHORT"
mkdir -p docs/qa data docs/qa/live-evidence docs/qa/sdk-contract-reports
rm -rf data docs/qa/live-evidence docs/qa/sdk-contract-reports
ln -s /opt/option-data-manager/shared/data data
ln -s /opt/option-data-manager/shared/docs-qa-live-evidence docs/qa/live-evidence
ln -s /opt/option-data-manager/shared/sdk-contract-reports docs/qa/sdk-contract-reports

if [ ! -f /opt/option-data-manager/shared/.env ]; then
  cp .env.example /opt/option-data-manager/shared/.env
  chmod 600 /opt/option-data-manager/shared/.env
fi
ln -sfn /opt/option-data-manager/shared/.env .env
printf "%s\n" "$SHA" > .release-sha
```

If the server cannot reach GitHub reliably, fetch `origin/main` on a trusted
local machine, verify the SHA, and stream `git archive <sha>` to the server with
SSH. Do not use a dirty worktree for normal deployment.

## Install Dependencies And Verify

```bash
cd "/opt/option-data-manager/releases/$SHORT"

UV_DEFAULT_INDEX=https://mirrors.aliyun.com/pypi/simple \
UV_HTTP_TIMEOUT=120 \
UV_PYTHON_INSTALL_DIR=/opt/option-data-manager/shared/uv-python \
  /root/.local/bin/uv sync --dev --python 3.11

chown -R odm:odm "/opt/option-data-manager/releases/$SHORT" /opt/option-data-manager/shared

.venv/bin/python --version
.venv/bin/pytest -q
.venv/bin/python -m compileall -q src tests option_data_manager
.venv/bin/python scripts/smoke-local-app.py --database data/server-smoke-"$SHORT".sqlite3
bash scripts/agentic-sdlc/check-agentic-sdlc.sh
```

The 2026-05-11 test-server deployment of `main@8e7f1c101447` passed:

```text
77 passed
compileall passed
smoke-local-app passed
agentic-sdlc check passed
```

## systemd Service

Create `/etc/systemd/system/odm-webui.service`:

```ini
[Unit]
Description=Option Data Manager WebUI
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=odm
Group=odm
WorkingDirectory=/opt/option-data-manager/current
Environment=ODM_DATABASE_PATH=/opt/option-data-manager/shared/data/option-data-current.sqlite3
Environment=ODM_WEB_HOST=127.0.0.1
Environment=ODM_WEB_PORT=8765
Environment=PYTHONUNBUFFERED=1
ExecStart=/opt/option-data-manager/current/.venv/bin/odm-webui
Restart=on-failure
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ReadWritePaths=/opt/option-data-manager/shared /home/odm/.config/option-data-manager /opt/option-data-manager/releases

[Install]
WantedBy=multi-user.target
```

Switch the release and start the service:

```bash
ln -sfn "/opt/option-data-manager/releases/$SHORT" /opt/option-data-manager/current
printf "%s\n" "$SHA" > /opt/option-data-manager/current-release.txt
chown -h odm:odm /opt/option-data-manager/current

systemctl daemon-reload
systemctl enable odm-webui
systemctl restart odm-webui
systemctl status odm-webui --no-pager
```

For a temporary public test server only, change the binding:

```bash
sed -i 's/^Environment=ODM_WEB_HOST=.*/Environment=ODM_WEB_HOST=0.0.0.0/' \
  /etc/systemd/system/odm-webui.service
systemctl daemon-reload
systemctl restart odm-webui
```

If `ufw` is active on the server, allow the test port:

```bash
ufw allow 8765/tcp
ufw status verbose
```

Cloud providers can also block the port at the security-group/firewall layer.
Allow inbound `8765/tcp` there only for the temporary test window.

Public `8765/tcp` exposure is not suitable for production. Prefer SSH tunnel,
VPN, Cloudflare Access, or a reverse proxy with HTTPS and authentication.

## Credentials

Open `/settings` in the WebUI and save TQSDK credentials there. On Linux, the
saved password is encrypted with the service user's Fernet key:

```text
/home/odm/.config/option-data-manager/secret.key
```

Use the same service user for command-line credential checks:

```bash
cd /opt/option-data-manager/current
sudo -u odm HOME=/home/odm \
  .venv/bin/odm-test-tqsdk \
  --database /opt/option-data-manager/shared/data/option-data-current.sqlite3 \
  --attempts 1 \
  --retry-delay 1
```

Do not run credential-dependent commands as `root` after saving credentials
through the WebUI, because `root` has a different Fernet key and cannot decrypt
the `odm` user's saved password.

## Initial Runtime Start

After a fresh deployment, verify the WebUI/API:

```bash
curl -sS http://127.0.0.1:8765/api/health
curl -sS http://127.0.0.1:8765/api/status
curl -sS http://127.0.0.1:8765/api/settings
```

Start or resume bounded background catch-up:

```bash
curl -sS -X POST http://127.0.0.1:8765/api/refresh
```

Start realtime Quote/Kline subscriptions:

```bash
curl -sS -X POST -H "Content-Type: application/json" -d '{}' \
  http://127.0.0.1:8765/api/quote-stream/start
```

Current realtime startup always performs a parent-process contract discovery
pass before spawning workers, then workers periodically refresh/reconcile the
active contract universe during long-running operation.

Check progress:

```bash
curl -sS http://127.0.0.1:8765/api/quote-stream
curl -sS http://127.0.0.1:8765/api/status
ps -eo pid,ppid,user,etime,cmd | grep -E 'quote_stream|metrics_worker|odm-webui' | grep -v grep
```

During closed sessions, `network health` can show a quiet/closed-session state.
That is expected and should not be treated as a live network alarm.

## Public Test Smoke

If the service is intentionally exposed on a test server:

```bash
for path in / /api/health /api/status /api/settings /api/webui/overview /assets/webui.js /assets/webui.css; do
  code=$(curl -sS --connect-timeout 8 --max-time 15 -o /tmp/odm-public-smoke.out -w '%{http_code}' "http://SERVER_IP:8765${path}" || true)
  printf '%s %s\n' "$code" "$path"
  test "$code" = 200
done
```

If local curl succeeds but public curl fails, check cloud security-group rules
for `8765/tcp` and the server firewall.

## Safe Upgrade Gate

Do not treat `systemctl restart odm-webui` as a complete upgrade by itself. The
WebUI starts and stops child quote-stream and metrics-worker processes, and the
shared SQLite database can also contain interrupted batch state from an earlier
runtime. A safe server upgrade must explicitly quiesce the old runtime, switch
the release, restart runtime services, and then prove the current-slice data is
ready.

Prepare and verify the new release directory first, then quiesce the old
runtime before switching `/opt/option-data-manager/current`:

```bash
BASE_URL=http://127.0.0.1:8765
DB=/opt/option-data-manager/shared/data/option-data-current.sqlite3

curl -fsS -X POST "$BASE_URL/api/quote-stream/stop" || true
curl -fsS -X POST "$BASE_URL/api/contract-manager/stop" || true
sleep 5

systemctl stop odm-webui

pkill -TERM -f 'option_data_manager.cli.quote_stream' || true
pkill -TERM -f 'option_data_manager.cli.metrics_worker' || true
sleep 2
pkill -KILL -f 'option_data_manager.cli.quote_stream' || true
pkill -KILL -f 'option_data_manager.cli.metrics_worker' || true

if pgrep -af 'odm-webui|option_data_manager.cli.quote_stream|option_data_manager.cli.metrics_worker'; then
  echo "Old ODM runtime process is still alive; aborting release switch." >&2
  exit 1
fi

if command -v fuser >/dev/null 2>&1; then
  fuser -v "$DB" 2>&1 || true
fi
```

After the old runtime is quiet, switch `current`, start systemd, and restore the
runtime services:

```bash
ln -sfn "/opt/option-data-manager/releases/$SHORT" /opt/option-data-manager/current
printf "%s\n" "$SHA" > /opt/option-data-manager/current-release.txt
chown -h odm:odm /opt/option-data-manager/current

systemctl daemon-reload
systemctl start odm-webui
curl -fsS "$BASE_URL/api/health"

curl -fsS -X POST "$BASE_URL/api/contract-manager/start"
curl -fsS -X POST "$BASE_URL/api/refresh"
curl -fsS -X POST -H "Content-Type: application/json" -d '{}' "$BASE_URL/api/quote-stream/start"

ps -eo pid,ppid,user,etime,cmd \
  | grep -E 'odm-webui|option_data_manager[.]cli[.](quote_stream|metrics_worker)' \
  | grep -v grep
```

Before the deployment can be recorded as complete, run a data-readiness gate.
This gate exists because realtime subscription progress only proves that Quote
and K-line objects were created; it does not prove `kline_20d_current` or
`option_source_metrics_current` have been populated.

```bash
cd /opt/option-data-manager/current
DB=/opt/option-data-manager/shared/data/option-data-current.sqlite3

.venv/bin/python - "$DB" <<'PY'
from __future__ import annotations

from datetime import UTC, datetime, timedelta
import sqlite3
import sys

db = sys.argv[1]
now = datetime.now(UTC)
connection = sqlite3.connect(db)
connection.row_factory = sqlite3.Row

row = connection.execute(
    """
    SELECT
        SUM(CASE WHEN stale = 0 THEN 1 ELSE 0 END) AS active_batches,
        SUM(CASE WHEN stale = 0 AND status = 'pending' THEN 1 ELSE 0 END) AS pending_batches,
        SUM(CASE WHEN stale = 0 AND status = 'running' THEN 1 ELSE 0 END) AS running_batches,
        SUM(CASE WHEN stale = 0 AND status = 'failed' THEN 1 ELSE 0 END) AS failed_batches,
        SUM(CASE WHEN stale = 0 AND status = 'success' THEN 1 ELSE 0 END) AS success_batches
    FROM collection_plan_batches
    WHERE plan_scope = 'routine-market-current-slice'
    """
).fetchone()

active = int(row["active_batches"] or 0)
pending = int(row["pending_batches"] or 0)
running = int(row["running_batches"] or 0)
failed = int(row["failed_batches"] or 0)
success = int(row["success_batches"] or 0)

stale_running = []
for item in connection.execute(
    """
    SELECT underlying_symbol, batch_index, updated_at
    FROM collection_plan_batches
    WHERE plan_scope = 'routine-market-current-slice'
      AND stale = 0
      AND status = 'running'
    """
):
    updated = datetime.fromisoformat(str(item["updated_at"]))
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=UTC)
    if updated < now - timedelta(minutes=30):
        stale_running.append(item)

issues = []
if stale_running:
    labels = ", ".join(
        f"{item['underlying_symbol']}#{item['batch_index']}@{item['updated_at']}"
        for item in stale_running[:10]
    )
    issues.append(f"stale running batch exists: {labels}")
if pending or running or failed:
    issues.append(
        f"current-slice incomplete: success={success}, active={active}, "
        f"pending={pending}, running={running}, failed={failed}"
    )

if issues:
    for issue in issues:
        print(f"NO-GO: {issue}", file=sys.stderr)
    sys.exit(1)

print(
    "GO: routine-market-current-slice complete "
    f"success={success}, active={active}, pending=0, running=0, failed=0"
)
PY
```

If the gate fails, the deployment is still live but it is not complete. Record it
as degraded in `docs/operations/deployment-log.md`, reset or resume the affected
batch work, and keep monitoring until the gate passes.

## Upgrade

1. Resolve the target GitHub commit SHA.
2. Create and verify a new release directory.
3. Run the Safe Upgrade Gate quiesce step so old workers and SQLite holders are
   not carried across the switch.
4. Switch `/opt/option-data-manager/current`.
5. Start `odm-webui`.
6. Re-run local and public smoke.
7. Restart contract manager, background refresh, realtime subscriptions, and the
   metrics worker path.
8. Run the Safe Upgrade Gate data-readiness step before marking the deployment
   complete.

```bash
systemctl start odm-webui
curl -sS -X POST http://127.0.0.1:8765/api/contract-manager/start
curl -sS -X POST http://127.0.0.1:8765/api/refresh
curl -sS -X POST -H "Content-Type: application/json" -d '{}' \
  http://127.0.0.1:8765/api/quote-stream/start
```

## Rollback

List releases and point `current` back to the previous one:

```bash
find /opt/option-data-manager/releases -maxdepth 1 -mindepth 1 -printf '%f\n' | sort
ln -sfn /opt/option-data-manager/releases/<previous-short-sha> /opt/option-data-manager/current
cat /opt/option-data-manager/current/.release-sha > /opt/option-data-manager/current-release.txt
chown -h odm:odm /opt/option-data-manager/current
systemctl restart odm-webui
```

Rollback changes code only. The shared SQLite database is intentionally
preserved. Restore a database backup separately if a data migration or runtime
operation needs to be undone.

## Operations Commands

```bash
systemctl status odm-webui --no-pager
journalctl -u odm-webui -f
systemctl restart odm-webui
cat /opt/option-data-manager/current-release.txt
readlink -f /opt/option-data-manager/current
ss -ltnp | grep :8765
```

## Troubleshooting

### `/api/quote-stream/stop` Returns 500

Check the service log first:

```bash
journalctl -u odm-webui -n 240 --no-pager
```

If the traceback contains `sqlite3.OperationalError: database is locked`, inspect
which process holds the SQLite file:

```bash
ps -eo pid,ppid,user,etime,stat,cmd | grep -E 'odm-|option_data_manager|python' | grep -v grep
ls -lh /opt/option-data-manager/shared/data/option-data-current.sqlite3*
fuser -v /opt/option-data-manager/shared/data/option-data-current.sqlite3 2>&1 || true
```

The API now treats quote-stream status cleanup and stop-state persistence as
best-effort when SQLite is temporarily busy, so stop should still return 200 and
terminate quote/metrics workers. If the server is running an older build, restart
`odm-webui` to clear a stale SQLite journal, then deploy a build that includes
the SQLite-lock tolerant realtime controls:

```bash
systemctl restart odm-webui
curl -sS -X POST http://127.0.0.1:8765/api/quote-stream/stop
```

After a successful stop, only `odm-webui` should remain:

```bash
ps -eo pid,ppid,user,etime,cmd | grep -E 'quote_stream|metrics_worker|odm-webui' | grep -v grep
```

For non-systemd local or WSL runs from the repository checkout, prefer the
bundled lifecycle script:

```bash
scripts/odm-server.sh status
scripts/odm-server.sh restart
scripts/odm-server.sh stop
```

It asks the API to stop realtime workers first, writes stale worker stop-files,
then falls back to terminating `quote_stream`, `metrics_worker`, and `odm-webui`
processes before starting a clean WebUI process.

Use SSH tunneling for private access without opening a public port:

```bash
ssh -L 8765:127.0.0.1:8765 root@SERVER_IP
```

Then open:

```text
http://127.0.0.1:8765
```
