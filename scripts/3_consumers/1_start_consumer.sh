kafka_server_1=127.0.0.1:9092
topic=first_topic
kafka-console-consumer.sh --bootstrap-server $kafka_server_1 --topic $topic --from-beginning 
# By adding --from-beginning option consumer not only shows current incomming messages but also all past messages. Note that messages are not ordered, to see total orderering try to see some particular partition where you will see total ordering. 
