# Kafka Examples

## Requisites

* Start Zookeeper
* Start Kafka

## Running a producer

```shell script
./gradlew Kafka-basics:producer 
```

## Running a consumer

```shell script
./gradlew Kafka-basics:consumer
```

## Running a consumer Group

```shell script
./gradlew Kafka-basics:consumerGroup 
```

## Pulling from a Twitter api and sending to a topic

```shell script
./gradlew TwitterProducer:producerFromTwitter
```
## Consuming from a topic and sending to ELK

```shell script
./gradlew ElasticConsumer:consumerSendToElk
```

## Running a Filter

* Pre requisite: Create a topic named "important_tweets"

```shell script
zookeeper_url=127.0.0.1:2181
topic_name=important_tweets
partitions=6
replicas=1
kafka-topics.sh --zookeeper $zookeeper_url --topic $topic_name --create --partitions $partitions --replication-factor $replicas
```

* Run the code that pull data from twitter and pushes it onto a topic

```shell script
./gradlew TwitterProducer:producerFromTwitter
```

* Run the filter that reads from a topic and pushes it onto another one ("important_tweets")
```shell script
./gradlew Kafka-streams-filter-tweets:streamsFilterTweets  
```

```shell script
kafka_server_1=127.0.0.1:9092
topic=important_tweets
group_name=my-group
kafka-console-consumer.sh --bootstrap-server $kafka_server_1 --topic $topic --from-beginning
```