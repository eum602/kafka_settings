zookeeper_url=127.0.0.1:2181
topic_name=twitter_tweets #first_topic

kafka-topics.sh --zookeeper $zookeeper_url --topic $topic_name --describe


