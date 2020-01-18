zookeeper_url=127.0.0.1:2181
topic_name=first_topic
kafka-topics.sh --zookeeper $zookeeper_url --topic $topic_name --delete

#In kafka --delete only deletes the topic if delete.topic.enable is set  to true otherwise it only marks the topic for deletion.

