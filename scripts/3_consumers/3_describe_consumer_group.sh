kafka_server_1=127.0.0.1:9092
topic=first_topic
group_name=my-group
kafka-consumer-groups.sh --bootstrap-server $kafka_server_1 --describe --group $group_name

#by running this script you will see a table with a header like this:
: <<'END'
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                              HOST            CLIENT-ID
my-group        first_topic     0          7               7               0               consumer-my-group-1-76fc1296-e549-4f74-9a97-246f1ad1411e /127.0.0.1      consumer-my-group-1
my-group        first_topic     1          8               8               0               consumer-my-group-1-76fc1296-e549-4f74-9a97-246f1ad1411e /127.0.0.1      consumer-my-group-1
my-group        first_topic     2          7               7               0               consumer-my-group-1-76fc1296-e549-4f74-9a97-246f1ad1411e /127.0.0.1      consumer-my-group-1
END

#In this table "LAG" indicates the difference between 'current_offset' and 'log_end_offset' thus indicates how much offsets behind the current offset a consumer have to read in some partition.
