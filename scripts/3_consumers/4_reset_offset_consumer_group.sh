kafka_server_1=127.0.0.1:9092
topic=twitter_tweets #first_topic
group_name=kafka-elastic-search

kafka-consumer-groups.sh --bootstrap-server $kafka_server_1 --group $group_name --reset-offsets --to-earliest --execute --topic $topic

: <<'END'
By using --reset-offset you have to specify the from where to reset the  offseto. When running the consumer will read the data from the modified current_offset in all partitions. There are many options for that: --to-earliest, to-latest, etc.
--to-earliest ==> resets CURRENT-OFFSET to ZERO
--shift-by 2 ; -2; -3, -6 and so on. THis allows to move forward or backwards the current_offset.
--execute => to update the offset reset , you can check this by simply using script 3_describe_consumer_group.sh
--all-topics or --topic
END

