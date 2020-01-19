zookeeper_url=127.0.0.1:2181
topic_name=first_topic
partitions=3
replicas=1
kafka-topics.sh --zookeeper $zookeeper_url --list
#In kafka it is not possible to create a topic with a replication factor greater of number of brokers you have.

