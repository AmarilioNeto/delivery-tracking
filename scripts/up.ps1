param()

Write-Host "Bringing up delivery-tracking stack..."
Push-Location -Path (Split-Path -Parent $MyInvocation.MyCommand.Definition)
Set-Location ..\
docker-compose up -d
Write-Host "Waiting a few seconds for services to start..."
Start-Sleep -Seconds 10
Write-Host "Done. Use docker ps to see containers."
Pop-Location
