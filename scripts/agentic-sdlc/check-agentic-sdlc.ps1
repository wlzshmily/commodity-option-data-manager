param(
    [string]$Root = "."
)

$required = @(
    "AGENTS.md",
    "PROJECT-CONTEXT.md",
    "docs/project-index.md",
    "docs/project-status.md",
    "docs/tasks/task-registry.md",
    "docs/tasks/progress-board.md",
    "docs/tasks/agent-handoffs.md",
    "docs/governance/risk-register.md",
    "docs/release/release-plan.md",
    "docs/traceability/requirements-traceability-matrix.md",
    "pyproject.toml"
)

$missing = @()
foreach ($path in $required) {
    if (-not (Test-Path -LiteralPath (Join-Path $Root $path))) {
        $missing += $path
    }
}

if ($missing.Count -gt 0) {
    Write-Error ("Missing required files: " + ($missing -join ", "))
    exit 1
}

Write-Host "agentic-sdlc structure check passed."

