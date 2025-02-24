from confluent_kafka import Consumer, KafkaException, KafkaError

def create_kafka_consumer():
    try:
        # Configurazione del producer Kafka con autenticazione
        kafka_config = {
            "bootstrap.servers": "confluent.eks-dev.eu-central-1.aws.cervedgroup.com:9093",
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": "kafka_client",
            "sasl.password": "kafka_client-secret",
            "client.id": "python-producer",
            "acks": "all",  # Garantisce che il messaggio venga ricevuto dai broker
        }

        # Creazione del producer Kafka
        return ApeConsumer(kafka_config)
    except Exception as e:
        print(f"Errore nella creazione del consumer Kafka: {str(e)}")
        sys.exit(1)

def consume(topic: str, consumer: Consumer):
    """
    Consuma messaggi dal topic Kafka specificato utilizzando il consumer fornito.

    :param topic: Nome del topic Kafka
    :param consumer: Istanza del consumer Kafka
    """
    consumer.subscribe([topic])
    print(f"Consumatore in ascolto sul topic: {topic}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Attesa messaggi per 1 secondo
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            # Stampare il messaggio ricevuto
            print(f"Ricevuto messaggio: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        print("\nChiusura consumer...")
    finally:
        consumer.close()
