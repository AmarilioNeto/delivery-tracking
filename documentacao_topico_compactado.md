# Documentação: Criação do Tópico Otimizado (Log Compaction)

Para atender ao requisito do desafio:
> "O sistema deve ser configurado de forma que o Kafka não armazene o histórico infinito de posições, mas sim a última localização conhecida por entregador."

Devemos criar o tópico `driver-locations` com a política de limpeza (cleanup.policy) definida como `compact`. Com isso, o Kafka manterá apenas a mensagem mais recente para cada `chave` (no nosso caso, o `driver_id`).

### Comando para criação do tópico:
Execute este comando dentro de qualquer container do Kafka (ex: `kafka1`):

```bash
docker exec -it kafka1 kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic driver-locations \
  --partitions 3 \
  --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=10000
```

### Explicação das configurações:
- `--partitions 3`: Distribui a carga entre os 3 brokers.
- `--replication-factor 3`: Garante tolerância a falhas (mesmo se 2 brokers caírem, os dados não são perdidos).
- `--config cleanup.policy=compact`: Ativa o "Log Compaction" no tópico, descartando o histórico infinito e mantendo apenas a última posição.
- `--config min.cleanable.dirty.ratio=0.01` e `--config segment.ms=10000`: Força o Kafka a compactar os logs mais agressivamente e de forma rápida, ideal para testes de pós-graduação e visualização em tempo real (em produção não é necessário ser tão agressivo).
