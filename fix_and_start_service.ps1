# TT1 Index UTBot Windows Service Fix and Start Script
# Please run this script in an Elevated PowerShell (Administrator) window.

$nssmPath = "C:\ProgramData\chocolatey\lib\NSSM\tools\nssm.exe"
$serviceName = $env:TTBOT_SERVICE_NAME
if ([string]::IsNullOrWhiteSpace($serviceName)) {
    $serviceName = "tt1-index-utbot"
}
$appDir = $PSScriptRoot
$venvPython = Join-Path $appDir "venv\Scripts\python.exe"
$appParams = "main.py"

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "TT1 Index UTBot Windows Service Fix and Start" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan

# Check for administrative privileges
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Error "CRITICAL: This script must be run as Administrator! Please open PowerShell as Administrator and run it again."
    Exit
}

if (-not (Test-Path $nssmPath)) {
    Write-Error "CRITICAL: NSSM not found at $nssmPath"
    Exit
}

if (-not (Test-Path $venvPython)) {
    Write-Error "CRITICAL: Python not found at $venvPython"
    Exit
}

# 1. Stop the service if running
Write-Host "Stopping service $serviceName (if running)..." -ForegroundColor Yellow
& $nssmPath stop $serviceName 2>$null
Start-Sleep -Seconds 2

# 2. Install the service if missing, then update configuration to use this workspace.
$existingService = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
if (-not $existingService) {
    Write-Host "Installing service $serviceName..." -ForegroundColor Yellow
    & $nssmPath install $serviceName $venvPython $appParams
}

Write-Host "Configuring service to use virtual environment Python..." -ForegroundColor Yellow
Write-Host "Setting Application -> $venvPython"
& $nssmPath set $serviceName Application $venvPython

Write-Host "Setting AppParameters -> $appParams"
& $nssmPath set $serviceName AppParameters $appParams

Write-Host "Setting AppDirectory -> $appDir"
& $nssmPath set $serviceName AppDirectory $appDir
& $nssmPath set $serviceName DisplayName "TT1 Index UTBot"
& $nssmPath set $serviceName Description "TT1 Index UTBot dashboard and scanner from $appDir"
& $nssmPath set $serviceName Start SERVICE_AUTO_START

# 3. Start the service
Write-Host "Starting service $serviceName..." -ForegroundColor Yellow
& $nssmPath start $serviceName

# 4. Check and display status
Start-Sleep -Seconds 3
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "Service Status:" -ForegroundColor Cyan
& sc.exe query $serviceName
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "Done! If the service started successfully, please visit http://localhost:7000 to view the dashboard." -ForegroundColor Green
