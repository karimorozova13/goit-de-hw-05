from kafka import KafkaConsumer
from configs import kafka_config
import json

from humidity_alerts_producer import produce_humidity_alerts
from temperature_alerts_producer import produce_temperature_alerts

consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_consumer_group_3'
)

my_name = "karine"
topic_name = f'{my_name}_building_sensors'

consumer.subscribe([topic_name])

print(f"Subscribed to topic '{topic_name}'")

try:
    for message in consumer:
        if message.value['temperature'] > 40:
            data = {**message.value, "alert": f"Temperature alert: {message.value}"}
            produce_temperature_alerts(data=data)
            print(f"Temperature alert: {message.value}")
        elif message.value['humidity'] > 80 or message.value['humidity'] < 20:
            data = {**message.value, "alert": f"Humidity alert: {message.value}"}
            produce_humidity_alerts(data=data)
            print(f"Humidity alert: {message.value}")
        else:
            print(f"Received message: {message.value}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()

