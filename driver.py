from consumer import Consumer
import time
from random import randint


def main():
    conf = {"group.id": "mygroup-qq%i" % randint(0, 21213321),
            "metadata.broker.list": "localhost",
            #"topic.auto.offset.reset": "earliest",
            #"topic.auto.commit.enable": "false",
            "auto.commit.enable": "false",
            "auto.offset.reset": "earliest",
            }

    w = Consumer('test', **conf)
    start = time.time()
    for msg in w.consume():
        print('message: %s' % msg.decode('utf-8'))
        end = time.time()
        if end - start >= 60:
            break
        # break
        # if msg == 'exit':
        #    break
    w.close()


def get_n_messages(n):
    conf = {"group.id": "mygroup-qq",
            "metadata.broker.list": "localhost",
            #"topic.auto.offset.reset": "earliest",
            #"topic.auto.commit.enable": "false",
            "auto.commit.enable": "false",
            "auto.offset.reset": "earliest",
            }

    w = Consumer('test', **conf)
    for msg in w.consume():
        print('message: %s' % msg.decode('utf-8'))
        n -= 1
        if n == 0:
            break

# get_n_messages(10)
main()
