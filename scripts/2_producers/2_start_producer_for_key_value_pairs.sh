broker_url=127.0.0.1:9092
topic_name=first_topic
kafka-console-producer.sh --broker-list $broker_url --topic $topic_name \
--producer-property acks=all \
--property parse.key=true --property key.separator=,

#examples how to enter data somekey, some value
