from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from typing import Dict, Any
import json
import os
from configparser import ConfigParser

class ApeProducer:

    

    def create_kafka_config_from_file(config_file_name: str) -> dict:

        config_file = ApeProducer.find_file_path(config_file_name)
        
        if config_file:
            print(f"File trovato: {config_file}")
        else:
            print("File non trovato")        
            raise FileNotFoundError(f"File di configurazione {config_file} non trovato")
    
        # Leggi il file di configurazione
        raw_config = {}
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and not line.startswith('['):
                    try:
                        key, value = line.split('=', 1)
                        raw_config[key.strip()] = value.strip()
                    except ValueError:
                        print(f"Impossibile analizzare la riga: {line}")
    
        # Crea la configurazione del client Kafka
        kafka_config = {
            'bootstrap.servers': raw_config.get('KAFKA.CLOUD.BOOTSTRAP.SERVERS'),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': raw_config.get('KAFKA.INNOVATIVE.USER'),
            'sasl.password': raw_config.get('KAFKA.INNOVATIVE.PASSWORD'),
        }
    
        # Crea la configurazione del client Schema Registry
        schema_registry_config = {
            'url': raw_config.get('KAFKA.CLOUD.SCHEMA.REGISTRY.HOST'),
            'basic.auth.user.info': f"{raw_config.get('KAFKA.CLOUD.SCHEMA.REGISTRY.USER')}:{raw_config.get('KAFKA.CLOUD.SCHEMA.REGISTRY.PASSWORD')}"
        }
    
        return {
            'kafka_config': kafka_config,
            'schema_registry_config': schema_registry_config
        }
    
    def find_file_path(filename: str) -> str:
        current_dir = os.path.abspath(os.path.dirname(__file__))  # Directory dello script
    
        while True:
            # Controlla il file nella directory corrente
            candidate = os.path.join(current_dir, filename)
            if os.path.isfile(candidate):
                return candidate
            
            # Controlla nelle sottodirectory di un solo livello
            for subdir in os.listdir(current_dir):
                subdir_path = os.path.join(current_dir, subdir)
                if os.path.isdir(subdir_path):
                    candidate = os.path.join(subdir_path, filename)
                    if os.path.isfile(candidate):
                        return candidate
            
            # Salire di un livello nella gerarchia delle cartelle
            parent_dir = os.path.dirname(current_dir)
            if parent_dir == current_dir:  # Se siamo alla root, fermarsi
                break
            current_dir = parent_dir
        
        return None  # File non trovato

    def __init__(self, config_file: str = "conf_env.properties"):
        # Ottieni la configurazione dal file
        configs = ApeProducer.create_kafka_config_from_file(config_file)
        kafka_config = configs['kafka_config']
        schema_registry_config = configs['schema_registry_config']
        
        # Schema Registry client configuration
        schema_registry_client = SchemaRegistryClient(schema_registry_config)

        # Create Avro serializer
        value_serializer = AvroSerializer(
            schema_registry_client=schema_registry_client,
            schema_str=self._get_avro_schema(),
        )

        # Producer configuration
        producer_conf = {
            'bootstrap.servers': kafka_config['bootstrap.servers'],
            'key.serializer': StringSerializer('utf_8'),
            'value.serializer': value_serializer,
            'security.protocol': kafka_config['security.protocol'], 
            'sasl.mechanisms': kafka_config['sasl.mechanisms'],
            'sasl.username': kafka_config['sasl.username'],
            'sasl.password': kafka_config['sasl.password']
        }
        
        self.producer = SerializingProducer(producer_conf)
        self.topic = 'ape-topic'  # Potresti voler rendere questo configurabile

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

    def parse_ape_json(file_name: str) -> Dict[str, Any]:
        """Parse APE JSON file and create a single document"""
        ape_doc = {}
        file_path=os.path.join(os.path.dirname(__file__), file_name)
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
    producer = ApeProducer()
    
    # Example: Process an APE JSON file
    ape_file = "../tests/input/32888643.json"
    ape_data = ApeProducer.parse_ape_json(ape_file)
    
    try:
        # Use the file name or a unique identifier as the key
        key = f"ape-{ape_file.split('/')[-1]}"
        producer.produce_message(key, ape_data)
        producer.flush()
    except KeyboardInterrupt:
        print("Shutting down producer...")
    finally:
        producer.flush()