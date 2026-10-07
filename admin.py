from confluent_kafka.admin import AdminClient, NewTopic

config = {'bootstrap.servers': 'localhost:9092'}
admin_client = AdminClient(config)
topic = 'wikistreams'

admin_client.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
print(f"Topic '{topic}' validé.")