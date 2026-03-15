param(
    [string]$broker = 'kafka1',
    [string]$topic = 'driver-locations',
    [int]$partitions = 6,
    [int]$replication = 3
)

Write-Host "Creating topic $topic (partitions=$partitions replication=$replication) on $broker"
Push-Location -Path (Split-Path -Parent $MyInvocation.MyCommand.Definition)
Set-Location ..\
docker exec -it $broker bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $topic --partitions $partitions --replication-factor $replication --config cleanup.policy=compact --config min.cleanable.dirty.ratio=0.01"
Pop-Location
