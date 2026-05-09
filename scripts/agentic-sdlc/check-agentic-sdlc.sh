#!/usr/bin/env bash
set -euo pipefail

root="${1:-.}"

required=(
  "AGENTS.md"
  "PROJECT-CONTEXT.md"
  "docs/project-index.md"
  "docs/project-status.md"
  "docs/tasks/task-registry.md"
  "docs/tasks/progress-board.md"
  "docs/tasks/agent-handoffs.md"
  "docs/governance/risk-register.md"
  "docs/release/release-plan.md"
  "docs/traceability/requirements-traceability-matrix.md"
  "pyproject.toml"
)

missing=()
for path in "${required[@]}"; do
  if [[ ! -e "$root/$path" ]]; then
    missing+=("$path")
  fi
done

if (( ${#missing[@]} > 0 )); then
  printf 'Missing required files: %s\n' "${missing[*]}" >&2
  exit 1
fi

echo "agentic-sdlc structure check passed."
