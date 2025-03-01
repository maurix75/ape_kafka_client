import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from ape_producer import ApeProducer


    # Example usage
if __name__ == "__main__":
    producer = ApeProducer()
    
    # Example: Process an APE JSON file
    ape_file = "32888643.json"
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