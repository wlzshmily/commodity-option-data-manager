#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "This setup script is intended for WSL2 Ubuntu or Linux." >&2
  exit 1
fi

case "$(pwd -P)" in
  /mnt/c/*|/mnt/d/*|/mnt/e/*)
    if [[ "${ODM_ALLOW_WINDOWS_MOUNT:-0}" != "1" ]]; then
      cat >&2 <<'EOF'
Refusing to create a Linux virtual environment on a Windows-mounted path.

Clone this repository inside the WSL filesystem instead, for example:
  mkdir -p ~/projects
  cd ~/projects
  git clone https://github.com/wlzshmily/commodity-option-data-manager.git
  cd commodity-option-data-manager
  bash scripts/setup-wsl-ubuntu.sh

Set ODM_ALLOW_WINDOWS_MOUNT=1 only for quick experiments, not for normal development.
EOF
      exit 1
    fi
    ;;
esac

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required. Install Python 3.11+ in Ubuntu first." >&2
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is not installed. Install it in Ubuntu with:"
  echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"
  exit 1
fi

mkdir -p data docs/qa/live-evidence docs/qa/sdk-contract-reports
mkdir -p "${HOME}/.config/option-data-manager"
chmod 700 "${HOME}/.config/option-data-manager" || true

uv python install 3.11
uv sync --dev --python 3.11

if [[ ! -f .env ]]; then
  cp .env.example .env
  chmod 600 .env || true
fi

cat <<'EOF'
WSL2 Ubuntu setup complete.

Next:
  1. Edit .env or export TQSDK_ACCOUNT/TQSDK_PASSWORD in your shell.
  2. Start the WebUI:
       uv run odm-webui
  3. Open from Windows:
       http://127.0.0.1:8765/
EOF
