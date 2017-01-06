from pykafka import KafkaClient
import time


def main():
    client = KafkaClient(hosts="127.0.0.1:9092")
    topic = client.topics['test']
    consumer = topic.get_balanced_consumer(consumer_group='testgroup',
                                           auto_commit_enable=False,
                                           zookeeper_connect='127.0.0.1:2181',
                                           use_rdkafka=True)
    start = time.time()
    for msg in consumer:
        print('message: %s' % msg.value.decode('utf-8'))
        end = time.time()
        if end - start >= 60:
            break


def get_n_messages(n):
    client = KafkaClient(hosts="127.0.0.1:9092")
    topic = client.topics['test']
    consumer = topic.get_simple_consumer(use_rdkafka=True)
    for msg in consumer:
        print('message: %s' % msg.value.decode('utf-8'))
        n -= 1
        if n == 0:
            break
    return

main()
# get_n_messages(10)
