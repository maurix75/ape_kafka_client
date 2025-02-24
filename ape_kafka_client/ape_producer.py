from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from typing import Dict, Any
import json

import os
from configparser import ConfigParser

class ApeProducer:
    def __init__(self, config_file: str = "conf_env.properties"):
        # Load configuration from properties file
        kafka_config = self._load_config(config_file)
        
        # Schema Registry client configuration
        schema_registry_conf = {'url': kafka_config.get('schema.registry.url')}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        # Create Avro serializer
        value_serializer = AvroSerializer(
            schema_registry_client=schema_registry_client,
            schema_str=self._get_avro_schema(),
        )

        # Producer configuration
        producer_conf = {
            'bootstrap.servers': kafka_config.get('bootstrap.servers'),
            'key.serializer': StringSerializer('utf_8'),
            'value.serializer': value_serializer
        }
        # Add any additional Kafka configuration
        producer_conf.update(kafka_config)
        
        self.producer = SerializingProducer(producer_conf)
        self.topic = kafka_config.get('topic', 'ape-topic')

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

    def delivery_report(self, err, msg):
        """Delivery report handler for produced messages"""
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

    def produce_message(self, key: str, value: Dict[str, Any]):
        """Produce a message to the Kafka topic"""
        try:
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=value,
                on_delivery=self.delivery_report
            )
            # Wait up to 1 second for events. Callbacks will be invoked during
            # this method call if the message is acknowledged.
            self.producer.poll(1)

        except ValueError as e:
            print(f"Invalid input: {str(e)}")
        except Exception as e:
            print(f"Error producing message: {str(e)}")

    def flush(self):
        """Flush the producer"""
        self.producer.flush()

def parse_ape_json(file_path: str) -> Dict[str, Any]:
    """Parse APE JSON file and create a single document"""
    ape_doc = {}
    with open(file_path, 'r') as f:
        for line in f:
            try:
                field = json.loads(line)
                ape_doc[field['label']] = field['value']
            except json.JSONDecodeError:
                continue
    return ape_doc

# Example usage
if __name__ == "__main__":
    producer = APEProducer(
        bootstrap_servers='localhost:9092',
        schema_registry_url='http://localhost:8081'
    )
    
    # Example: Process an APE JSON file
    ape_file = "/path/to/your/ape_fields.json"
    ape_data = parse_ape_json(ape_file)
    
    try:
        # Use the file name or a unique identifier as the key
        key = f"ape-{ape_file.split('/')[-1]}"
        producer.produce_message(key, ape_data)
        producer.flush()
    except KeyboardInterrupt:
        print("Shutting down producer...")
    finally:
        producer.flush()