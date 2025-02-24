import os
from confluent_kafka import Producer, KafkaException
import json
import sys
import configparser

# Funzione per leggere la configurazione da un file .properties
def read_config(file_path):
    """Legge le configurazioni dal file .properties"""
    config = configparser.ConfigParser()
    config.read(file_path)
    
    # Estrarre i valori dalla sezione 'kafka'
    kafka_config = {
        "bootstrap.servers": config.get('kafka', 'KAFKA_BROKER', fallback='default_broker_address'),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": config.get('kafka', 'KAFKA_USERNAME', fallback='default_username'),
        "sasl.password": config.get('kafka', 'KAFKA_PASSWORD', fallback='default_password'),
        "client.id": "python-producer",
        "acks": "all",
    }
    return kafka_config

# Creazione del producer Kafka con configurazione dal file
def create_kafka_producer(config_file='conf_env.properties'):
    """Crea e restituisce un'istanza di un producer Kafka utilizzando la configurazione dal file"""
    try:       

        if not os.path.isfile(config_file):
            print(f"Errore: Il file di configurazione '{config_file}' non esiste.")
            sys.exit(1)
        kafka_config = read_config(config_file)
        return Producer(kafka_config)
    except Exception as e:
        print(f"Errore nella creazione del producer Kafka: {str(e)}")
        sys.exit(1)

def produce(producer, topic, message):
    """Invia un messaggio al topic Kafka specificato"""
    try:
        # Serializzazione del messaggio in formato JSON
        message_json = json.dumps(message)

        # Invio del messaggio al topic specificato
        future = producer.send(topic, value=message_json)

        # Attendere la conferma dell'invio
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


