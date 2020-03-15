zookeeper_url=127.0.0.1:2181
topic_name=twitter_tweets #first_topic
partitions=6
replicas=1
kafka-topics.sh --zookeeper $zookeeper_url --topic $topic_name --create --partitions $partitions --replication-factor $replicas
#In kafka it is not possible to create a topic with a replication factor greater of number of brokers you have.

