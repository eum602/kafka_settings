kafka_server_1=127.0.0.1:9092
topic=first_topic
group_name=my-group
kafka-consumer-groups.sh --bootstrap-server $kafka_server_1 --list
