param()

Write-Host "Tearing down delivery-tracking stack..."
Push-Location -Path (Split-Path -Parent $MyInvocation.MyCommand.Definition)
Set-Location ..\
docker-compose down -v
Write-Host "All containers stopped and volumes removed (if any)."
Pop-Location
