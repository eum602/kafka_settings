kafka_server_1=127.0.0.1:9092
topic=first_topic
kafka-console-consumer.sh --bootstrap-server $kafka_server_1 --topic $topic 
