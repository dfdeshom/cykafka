from consumer import Consumer
import time
from random import randint
conf = {"group.id": "mygroup-qq%i" % randint(0, 21213321),
        "metadata.broker.list": "localhost:9092,localhost:9093",
        "enable.auto.commit": "false",
        "auto.offset.reset": "earliest",
        #"queued.min.messages": "100000",  # default by want to match pykafka,

        # set by pyk
        # 'queued.max.messages.kbytes': '2048000',
        # 'receive.message.max.bytes': "3145728",
        # 'queued.min.messages': "2000",
        # 'fetch.message.max.bytes': "1048576",
        # 'fetch.wait.max.ms': "100",
        # 'fetch.min.bytes': "1",
        # 'socket.timeout.ms': "30000",
        }
topic = "test3"


def main():
    w = Consumer(topic, **conf)
    start = time.time()
    for msg in w.consume():
        print('message: %s' % msg.decode('utf-8'))
        end = time.time()
        if end - start >= 60:
            break

    w.close()


def get_n_messages(n):
    w = Consumer(topic, **conf)
    for msg in w.consume():
        print('message: %s' % msg.decode('utf-8'))
        n -= 1
        if n == 0:
            break

# get_n_messages(10)
main()
