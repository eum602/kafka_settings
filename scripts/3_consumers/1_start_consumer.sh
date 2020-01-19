kafka_server_1=127.0.0.1:9092
topic=first_topic
group_name=my-group
kafka-console-consumer.sh --bootstrap-server $kafka_server_1 --topic $topic --group $group_name --from-beginning
#adding --group with some name creates a set with all consumers who run the kafka consumer with the same group id. To test this functionality run this command in various terminals and send messages from a producer you will see the following:
	#if only one consumer is started then all messages from all partitions will go there.
	#if you increase to more than one xonsumer then all messages gets distributed to the consumers.
	# btw each consumer can subscribe to one or more producer. But if there is more consumers than partitions for some topic, then some of those excedent consumers will not read nothing(those consumerrs can be thought like backup consumer in case some of the active consumers fails)

# By adding --from-beginning option consumer not only shows current incomming
#messages but also all past messages. Note that messages are not ordered, to see total orderering try to see
#some particular partition where you will see total ordering.
#THE CONSUMER GROUP REBALANCE AND SHARE THE LOAD BETWEEN ALL THE CONSUMERS.

#According to consumer offsets, when some consumer reads data from kafka then a commit offset occurs. As a result, if we read all the data (--from-beginning) in some consumer that is into some group, then all other consumers into that group won't read again that message because the offset has moved forward.
