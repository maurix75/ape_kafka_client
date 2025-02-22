from confluent_kafka import Producer
import json
import sys

def create_kafka_producer():
    """Create and return a Kafka producer instance"""
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
        producer = Producer(kafka_config)
        return producer
    except Exception as e:
        print(f"Errore nella creazione del producer Kafka: {str(e)}")
        sys.exit(1)

def produce(producer, topic, message):
    """Send a message to specified Kafka topic"""
    try:
        future = producer.send(topic, value=message)
        # Attendiamo la conferma dell'invio
        record_metadata = future.get(timeout=10)
        print(f"Messaggio inviato con successo:")
        print(f"Topic: {record_metadata.topic}")
        print(f"Partition: {record_metadata.partition}")
        print(f"Offset: {record_metadata.offset}")
    except Exception as e:
        print(f"Errore nell'invio del messaggio: {str(e)}")