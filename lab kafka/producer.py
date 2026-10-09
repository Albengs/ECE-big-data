# %%
import socket
import time
from confluent_kafka import Producer
from datetime import datetime, timedelta


path = "Draclua.txt"

text = open(path,'r')

# %%
#print(datetime.now().strftime("%H:%M"))
# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

# %%
topic='book'
message=text.readline()
# %% Streaming Query
duration = 5 # Streaming window in minutes
start_time = datetime.now() # Current clock time
stop_time = start_time + timedelta(minutes=duration) #start+duration=stop

while message :
  
  producer.produce(
    topic=topic,
    value=message
  )
  #print(message)
  message=text.readline()
  time.sleep(0.02)

producer.flush()
producer.close()
