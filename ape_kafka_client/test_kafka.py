# Uso del producer Kafka per inviare messaggi
from kafka_producer import create_kafka_producer, produce


if __name__ == "__main__":
    producer = create_kafka_producer()    
    produce(producer,"ape.streaming", {"key_1": "value_1", "key_2": "value_2"})