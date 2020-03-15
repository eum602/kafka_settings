kafka_server_1=127.0.0.1:9092
topic=twitter_tweets
group_name=my-group

kafka-console-consumer.sh --bootstrap-server $kafka_server_1 --topic $topic --group $group_name --from-beginning \
--property print.key=true --property key.separator=,
