from confluent_kafka import Producer
import json
import time

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

# Funzione di callback per confermare la consegna
def delivery_report(err, msg):
    if err is not None:
        print(f"Errore nell'invio del messaggio: {err}")
    else:
        print(f"Messaggio inviato con successo: {msg.topic()} [{msg.partition()}]")

# Topic Kafka
topic = "ape.streaming"

# Invio di 10 messaggi di esempio
for i in range(10):
    message = {
        "id": i,
        "text": f"Messaggio {i}",
        "timestamp": time.time(),
    }
    producer.produce(
        topic,
        key=str(i),
        value=json.dumps(message),
        callback=delivery_report,
    )
    producer.poll(0)  # Processa eventuali callback

# Assicura che tutti i messaggi siano inviati prima di chiudere
producer.flush()

print("Produzione completata.")
