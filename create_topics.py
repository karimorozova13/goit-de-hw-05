from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

my_name = "karine"

building_sensors_topic_name = f'{my_name}_building_sensors'
temperature_alerts_topic_name = f'{my_name}_temperature_alerts'
humidity_alerts_topic_name = f'{my_name}_humidity_alerts'

num_partitions = 2
replication_factor = 1


building_sensors_topic = NewTopic(name=building_sensors_topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
temperature_alerts_topic = NewTopic(name=temperature_alerts_topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
humidity_alerts_topic = NewTopic(name=humidity_alerts_topic_name, num_partitions=num_partitions, replication_factor=replication_factor)


try:
    admin_client.create_topics(new_topics=[building_sensors_topic, temperature_alerts_topic, humidity_alerts_topic], validate_only=False)
    print(f"Topic '{building_sensors_topic_name}' created successfully.")
    print(f"Topic '{temperature_alerts_topic_name}' created successfully.")
    print(f"Topic '{humidity_alerts_topic_name}' created successfully.")

except Exception as e:
    print(f"An error occurred: {e}")

print(topic for topic in admin_client.list_topics() if "my_name" in topic)

admin_client.close()

