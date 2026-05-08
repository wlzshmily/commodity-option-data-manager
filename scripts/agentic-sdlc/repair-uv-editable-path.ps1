param(
    [string]$Root = "."
)

$resolvedRoot = (Resolve-Path -LiteralPath $Root).Path
$sitePackages = Join-Path $resolvedRoot ".venv\Lib\site-packages"
$pth = Join-Path $sitePackages "_editable_impl_option_data_manager.pth"
$src = Join-Path $resolvedRoot "src"

if (-not (Test-Path -LiteralPath $sitePackages)) {
    throw "Virtual environment site-packages not found: $sitePackages"
}
if (-not (Test-Path -LiteralPath $src)) {
    throw "Source directory not found: $src"
}

[System.IO.File]::WriteAllText(
    $pth,
    $src + [Environment]::NewLine,
    [System.Text.Encoding]::Default
)
Write-Host "Repaired editable path file: $pth"

