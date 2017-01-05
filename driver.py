from consumer import Consumer


def main():
    conf = {"group.id": "mygroup-qq",
            "metadata.broker.list": "localhost",
            "topic.auto.offset.reset": "earliest",
            "topic.auto.commit.enable": "false",
            "auto.commit.enable": "false",
            "auto.offset.reset": "earliest",
            }

    w = Consumer('test', **conf)
    for msg in w.consume():
        print msg
        # break
        # if msg == 'exit':
        #    break
    w.close()

main()
