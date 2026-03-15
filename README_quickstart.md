# Delivery Tracking - Quickstart

Este projeto fornece uma stack local para demonstrar uma pipeline Kafka -> Redis para rastreamento de entregadores.

Serviços incluídos:
- 3 x Apache Kafka 4.1.1 (KRaft)
- Kafka Connect
- Redis
- Kafdrop (UI)
- Prometheus
- Grafana

Passos rápidos (PowerShell)

1) Subir a stack
```powershell
cd 'kafka_classes\KAFKAPOS\delivery-tracking'
.\scripts\up.ps1
```

2) Criar tópico (compacted)
```powershell
.\scripts\create_topic.ps1 -broker kafka1 -topic driver-locations -partitions 6 -replication 3
```

3) Rodar o producer simulator (em venv)
```powershell
cd producer
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python producer_simulator.py --drivers 100 --interval 0.1
```

4) Instanciar o connector Redis (via REST)
```powershell
curl -X POST -H "Content-Type: application/json" --data @..\connect\redis-sink.json http://localhost:8083/connectors
```

5) Abrir interfaces
- Kafdrop: http://localhost:9000
- Grafana: http://localhost:3000 (admin/admin)

Validação rápida (no host)
- Ver chaves no redis:
```powershell
docker exec -it redis redis-cli GET driver:1
```

Observações
- Mensagens são enviadas com key = driver_id para garantir que todas as atualizações de um driver vão para a mesma partição. O tópico usa cleanup.policy=compact para manter apenas o estado mais recente por chave.
