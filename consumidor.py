import json
import redis
import sys
from confluent_kafka import Consumer, KafkaException, KafkaError

# ==============================================================================
# MICROSERVIÇO INTEGRADOR (KAFKA -> REDIS)
# Atua como substituto resiliente ao Kafka Connect (Redis Sink Connector)
# devido ao bug de biblioteca JNI "NoSuchFieldError: NETWORK_INTERFACES"
# na imagem Confluent 7.4.0 com o plugin jcustenborder.
# ==============================================================================

def main():
    # Conectando ao Redis (o espelho do estado atual)
    try:
        redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        redis_client.ping()
        print("✅ Conectado ao Redis com sucesso!")
    except redis.ConnectionError as e:
        print(f"❌ Erro crítico ao conectar no Redis: {e}")
        sys.exit(1)

    # Configuração do Consumidor Kafka (alta performance via confluent_kafka/C)
    TOPICO_KAFKA = 'driver-locations'
    servidores_kafka = 'localhost:29092,localhost:29093,localhost:29094'

    conf = {
        'bootstrap.servers': servidores_kafka,
        'group.id': 'grupo-sincronizador-redis',
        'auto.offset.reset': 'latest',
        # DESATIVADO o auto-commit para garantir tolerância a falhas.
        # Só comitamos o progresso após a certeza de que salvamos no banco de dados em memória.
        'enable.auto.commit': False 
    }

    consumidor = Consumer(conf)
    consumidor.subscribe([TOPICO_KAFKA])

    print(f"🎧 Microserviço Integrador rodando e escutando o tópico '{TOPICO_KAFKA}'...")

    try:
        while True:
            # Poll contínuo buscando novas mensagens
            mensagem = consumidor.poll(timeout=1.0)
            
            if mensagem is None:
                continue
            
            if mensagem.error():
                if mensagem.error().code() == KafkaError._PARTITION_EOF:
                    # Fim da partição atingido (não é um erro real)
                    continue
                else:
                    print(f"⚠️ Erro no Kafka: {mensagem.error()}")
                    continue
                    
            # 1. Desserialização Segura (Se o JSON vier quebrado, não crasha o microserviço)
            try:
                dados_brutos = mensagem.value().decode('utf-8')
                dados_entregador = json.loads(dados_brutos)
            except (json.JSONDecodeError, AttributeError) as e:
                print(f"⚠️ Mensagem corrompida ignorada: {e}")
                # Comita para pular a mensagem ruim e não travar a fila
                consumidor.commit(asynchronous=True) 
                continue
                
            id_entregador = dados_entregador.get('driver_id')
            
            if id_entregador:
                # 2. Chave Única para o Redis (Garante que só haja o último registro por entregador)
                chave_redis = f"entregador:{id_entregador}"
                valor_para_salvar = json.dumps(dados_entregador)
                
                try:
                    # 3. Operação SET (Idempotente): Sobrescreve sempre a localização antiga
                    redis_client.set(chave_redis, valor_para_salvar)
                    print(f"📍 [Kafka->Redis] Atualizado: {chave_redis} | Status: {dados_entregador.get('status')}")
                    
                    # 4. Commit Manual: Confirma para o cluster que finalizamos esta mensagem com sucesso
                    consumidor.commit(asynchronous=True)
                except redis.RedisError as e:
                    print(f"❌ Falha de gravação no Redis: {e}")
                    # Como não comitamos no Kafka, a mensagem será reprocessada
                    # caso o serviço reinicie. Isso é arquitetura resiliente!

    except KeyboardInterrupt:
        print("\n🛑 Serviço parado pelo usuário.")
    finally:
        consumidor.close()
        print("🔌 Conexões encerradas de forma segura.")

if __name__ == '__main__':
    main()
