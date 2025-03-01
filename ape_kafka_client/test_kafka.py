# Uso del producer Kafka per inviare messaggi
from ape_producer import ApeProducer


if __name__ == "__main__":
    producer = ApeProducer();
    producer.produce_message("ape.streaming", {"key_1": "value_1", "key_2": "value_2"})