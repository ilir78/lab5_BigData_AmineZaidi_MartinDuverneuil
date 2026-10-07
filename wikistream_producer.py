import json
import requests
from sseclient import SSEClient as EventSource
from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)
topic = 'wikistreams'
url = 'https://stream.wikimedia.org/v2/stream/recentchange'

print("Connexion au flux Wikimedia...")
response = requests.get(url, stream=True)
client = EventSource(response)

for event in client.events():
    if event.event == 'message':
        try:
            change = json.loads(event.data)
            
            
            if change.get("server_name") == "en.wikipedia.org":
                producer.produce(topic=topic, value=event.data.encode('utf-8'))
                print(f"Modif interceptée : {change.get('title')} par {change.get('user')}")
                producer.poll(0)
                
        except ValueError:
            pass