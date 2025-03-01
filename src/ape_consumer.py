from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from typing import Dict, Any

import os
from configparser import ConfigParser

class ApeConsumer:
    def __init__(self, config_file: str = "conf_env.properties"):
        # Load configuration from properties file
        kafka_config = self._load_config(config_file)
        
        # Schema Registry client configuration
        schema_registry_conf = {'url': kafka_config.get('schema.registry.url')}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        # Fetch the latest Avro schema
        avro_deserializer = AvroDeserializer(
            schema_registry_client=schema_registry_client,
            schema_str=self._get_avro_schema()
        )

        # Consumer configuration
        consumer_conf = {
            'bootstrap.servers': kafka_config.get('bootstrap.servers'),
            'group.id': kafka_config.get('group.id'),
            'auto.offset.reset': 'earliest',
            'key.deserializer': StringDeserializer('utf_8'),
            'value.deserializer': avro_deserializer
        }
        # Add any additional Kafka configuration
        consumer_conf.update(kafka_config)

        self.consumer = DeserializingConsumer(consumer_conf)
        self.topic = kafka_config.get('topic', 'ape-topic')
        self.running = False

    def _load_config(self, config_file: str) -> dict:
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Configuration file {config_file} not found")
            
        config = {}
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip()
        return config

    

    def _get_avro_schema(self) -> str:
        return """
        {
          "type": "record",
          "name": "APEDocument",
          "namespace": "it.ape.schema",
          "fields": [
            {"name": "destinazione_uso_residenziale", "type": "string"},
            {"name": "destinazione_uso_non_residenziale", "type": "string"},
            {"name": "destinazione_uso_classificazione", "type": "string"},
            {"name": "oggetto_attestato_intero_edificio", "type": "string"},
            {"name": "oggetto_attestato_unita_immobiliare", "type": "string"},
            {"name": "oggetto_attestato_gruppo_unita_immobiliare", "type": "string"},
            {"name": "oggetto_attestato_numero_unita", "type": "string"},
            {"name": "oggetto_attestato_nuova_costruzione", "type": "string"},
            {"name": "oggetto_attestato_passaggio_proprieta", "type": "string"},
            {"name": "oggetto_attestato_locazione", "type": "string"},
            {"name": "oggetto_attestato_ristrutturazione_importante", "type": "string"},
            {"name": "oggetto_attestato_riqualificazione_energetica", "type": "string"},
            {"name": "oggetto_attestato_altro", "type": "string"},
            {"name": "dati_identificativi_regione", "type": "string"},
            {"name": "dati_identificativi_comune", "type": "string"},
            {"name": "dati_identificativi_indirizzo", "type": "string"},
            {"name": "dati_identificativi_piano", "type": "string"},
            {"name": "dati_identificativi_interno", "type": "string"},
            {"name": "dati_identificativi_zona_climatica", "type": "string"},
            {"name": "dati_identificativi_anno_costruzione", "type": "string"},
            {"name": "dati_identificativi_superficie_riscaldata", "type": "string"},
            {"name": "dati_identificativi_superficie_raffrescata", "type": "string"},
            {"name": "dati_identificativi_volume_lordo_riscaldato", "type": "string"},
            {"name": "dati_identificativi_volume_lordo_raffrescato", "type": "string"},
            {"name": "dati_identificativi_gis_lat", "type": "string"},
            {"name": "dati_identificativi_gis_lon", "type": "string"},
            {"name": "dati_identificativi_provincia", "type": ["null", "double"]},
            {"name": "dati_identificativi_cod_istat", "type": ["null", "double"]},
            {"name": "dati_identificativi_cap", "type": ["null", "double"]}
          ]
        }
        """

    def start_consuming(self):
        """Start consuming messages from the Kafka topic"""
        try:
            self.consumer.subscribe([self.topic])
            self.running = True

            while self.running:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                # Process the message
                self.process_message(msg.key(), msg.value())

        finally:
            self.consumer.close()

    def process_message(self, key: str, value: Dict[str, Any]):
        """Process each consumed message"""
        print(f"Received message:")
        print(f"Key: {key}")
        print(f"Value: {value}")
        # Add your custom processing logic here

    def stop(self):
        """Stop consuming messages"""
        self.running = False

# Example usage
if __name__ == "__main__":
    consumer = APEConsumer(
        bootstrap_servers='localhost:9092',
        schema_registry_url='http://localhost:8081',
        group_id='ape-consumer-group',
        topic='ape-topic'
    )
    
    try:
        consumer.start_consuming()
    except KeyboardInterrupt:
        consumer.stop()