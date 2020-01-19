broker_url=127.0.0.1:9092
topic_name=topic_from_producer #first_topic
kafka-console-producer.sh --broker-list $broker_url --topic $topic_name \
--producer-property acks=all

#specifying a topic that is not created by way of kafka-topics can be created from here, but replicas and partition configuration will default to 1; this is not recomended, instead only refer to topics that were correctly created and configured by way of kafka-topics tool.
#To modify the default properties  go to ~/kafka/config/server.properties
#then modify this: num.partitions=1 ==> num.partitions=3 so now when a new topic is created, lets say by usin a producer then that topic will have by default three partitions. ==> after changing restart kafka
#By creating a topic by using producers will throw a warn after that you wil be ableto send messages.
#You can see the properties of a topic by listing the topics with kafka-topics ...--list
