from confluent_kafka import Consumer, KafkaError
import time

c = Consumer({'bootstrap.servers': 'localhost', 'group.id': 'mygroup-confluent',
              #"topic.auto.offset.reset": "earliest",
              #"topic.auto.commit.enable": "false",
              #"auto.commit.enable": "false",
              #"auto.offset.reset": "earliest",
              'default.topic.config': {'auto.offset.reset': 'earliest',
                                       "auto.commit.enable": "false",
                                       }})
c.subscribe(['test'])
running = True
start = time.time()
while running:
    msg = c.poll()
    if not msg.error():
        print('message: %s' % msg.value().decode('utf-8'))
    elif msg.error().code() != KafkaError._PARTITION_EOF:
        print(msg.error())
        running = False
    end = time.time()
    if end - start >= 60:
        running = False
c.close()
