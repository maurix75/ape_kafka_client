from confluent_kafka import Producer
import json
import sys
import os

kafka_config = {
    "bootstrap.servers": os.getenv("KAFKA_BROKER", "default_broker_address"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("KAFKA_USERNAME", "default_username"),
    "sasl.password": os.getenv("KAFKA_PASSWORD", "default_password"),
    "client.id": "python-producer",
    "acks": "all",
}


def create_kafka_producer():
    """Create and return a Kafka producer instance"""
    try:     
        # Creazione del producer Kafka        
        return Producer(kafka_config)
    except Exception as e:
        print(f"Errore nella creazione del producer Kafka: {str(e)}")
        sys.exit(1)


def produce(producer, topic, message):
    """Send a message to specified Kafka topic"""
    try:
        message_json = json.dumps(message)
        future = producer.send(topic, value=message_json)
        record_metadata = future.get(timeout=10)
        print(f"Messaggio inviato con successo:")
        print(f"Topic: {record_metadata.topic}")
        print(f"Partition: {record_metadata.partition}")
        print(f"Offset: {record_metadata.offset}")
    except KafkaException as e:
        print(f"Errore di Kafka: {str(e)}")
    except Exception as e:
        print(f"Errore generico: {str(e)}")
    finally:
        # Assicuriamoci che tutti i messaggi siano inviati
        producer.flush()
