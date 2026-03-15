# Documentação: Kafka Connect - Redis Sink

De acordo com o requisito:
> "Integração com Redis (Kafka Connect): Configure o Kafka Connect (Sink Connector) para realizar o transporte automático dos dados do Kafka para o Redis. Para cada driver_id, deve existir apenas um registro..."

Abaixo estão os comandos para instalar o plugin e instanciar o conector.

### 1. Garantir a instalação do Plugin do Redis
A maioria das imagens do Confluent (`cp-kafka-connect`) não vem com o Redis Sink por padrão. Você precisa garantir que o plugin `jcustenborder/kafka-connect-redis` esteja instalado na pasta `/opt/connectors` montada no seu `docker-compose.yml`.

Para baixar manualmente, você pode usar o comando (dentro do repositório da Confluent Hub) no terminal onde o docker está rodando:
```bash
docker exec -it connect confluent-hub install jcustenborder/kafka-connect-redis:latest --no-prompt
```
E depois reiniciar o container do connect:
```bash
docker restart connect
```

### 2. Comando para Instanciar o Conector (cURL)
Com o cluster e o Connect (porta 8083) rodando, e o arquivo `redis-sink-config.json` salvo na sua pasta, rode este comando cURL para enviar a configuração via API REST para o Kafka Connect:

```bash
curl -X POST -H "Content-Type: application/json" --data @redis-sink-config.json http://localhost:8083/connectors
```

### 3. Verificar o Status do Conector
Para ter certeza de que o conector está rodando e enviando os dados do tópico para o Redis:
```bash
curl http://localhost:8083/connectors/redis-sink-connector/status
```
Se a resposta contiver `"state":"RUNNING"`, a sua integração (Kafka -> Connect -> Redis) está 100% funcional.

*O uso deste conector anula e substitui a necessidade do script consumidor.py, atendendo diretamente ao requisito do desafio prático.*