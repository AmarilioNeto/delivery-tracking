# Guia Passo a Passo: Testando o Desafio da Pós-Graduação

Este guia cobre desde subir o ambiente até o momento de gravar o vídeo provando que todos os requisitos funcionam.

---

### Passo 1: Subir a Infraestrutura
Abra o terminal na pasta raiz do projeto (`C:\Users\AmarilioBLN\KafkaPos\kafka_classes\KAFKAPOS\delivery-tracking`) e inicie os containers:
```bash
docker compose down -v
docker compose up -d
```
*Espere uns 30 a 60 segundos para todos os brokers subirem e o Kafka Connect inicializar completamente.*

---

### Passo 2: Preparar o Kafka Connect (Instalar o Plugin do Redis)
O Kafka Connect precisa do conector do Redis, que não vem por padrão.
1. Instale o plugin rodando:
```bash
docker exec -it connect confluent-hub install jcustenborder/kafka-connect-redis:latest --no-prompt
```
2. Reinicie o Kafka Connect para ele carregar o plugin:
```bash
docker restart connect
```
*Aguarde mais uns 30 segundos.*

---

### Passo 3: Criar o Tópico Otimizado (Log Compaction)
Para atender à exigência de não guardar o histórico infinito:
```bash
docker exec -it kafka1 kafka-topics --create --bootstrap-server localhost:9092 --topic driver-locations --partitions 3 --replication-factor 3 --config cleanup.policy=compact --config min.cleanable.dirty.ratio=0.01 --config segment.ms=10000
```

---

### Passo 4: Instanciar a Integração Automática (O Sink Connector)
Em vez de rodar o `consumidor.py`, você vai mandar o Kafka Connect fazer o serviço sozinho lendo o arquivo JSON que criamos:
```bash
curl -X POST -H "Content-Type: application/json" --data @redis-sink-config.json http://localhost:8083/connectors
```
**Para confirmar que funcionou:**
```bash
curl http://localhost:8083/connectors/redis-sink-connector/status
```
*(Deve aparecer a mensagem dizendo que está `RUNNING`)*

---

### Passo 5: Gerar as Mensagens em Tempo Real (O Produtor)
Em um novo terminal na pasta do projeto, ative o seu ambiente virtual Python (se você tiver) e rode o simulador que ajustamos:
```bash
pip install confluent-kafka redis
python producer/producer_simulator.py --drivers 10 --interval 0.5
```
*(Deixe ele rodando. Ele vai simular as posições de 10 entregadores e mandar para o Kafka sem parar).*

---

### Passo 6: Validar no Banco Redis (A Prova Real)
Com o produtor rodando em uma tela, abra outro terminal e conecte-se ao Redis para provar a unicidade:
```bash
docker exec -it redis redis-cli
```
Dentro do Redis, rode:
```text
keys entregador:*
```
*(Isso vai mostrar a lista de chaves. Haverá no máximo 10 chaves, provando que não há duplicidade e não há histórico acumulando).*

Para ver os dados de um entregador específico mudando (digite isso várias vezes e veja a latitude/longitude ou timestamp mudando):
```text
GET entregador:5
GET entregador:5
GET entregador:5
```

---

### Passo 7: Teste de Resiliência para o Vídeo
Durante a gravação do seu vídeo, com o seu painel do Grafana aberto em `http://localhost:3000` e com o simulador Python rodando no terminal, mostre você parando um nó:
1. Abra um terminal e rode:
```bash
docker stop kafka2
```
2. Mostre o terminal do Produtor: Ele deve dar algumas travadas (retries) mas rapidamente vai continuar enviando as mensagens, porque configuramos o `--replication-factor 3`.
3. Mostre o Grafana: Vai cair o número de brokers vivos, os painéis vão indicar o alerta do `kafka2` caindo, mas os dados continuam fluindo porque os outros 2 brokers seguram a onda (Tolerância a Falhas provada!).