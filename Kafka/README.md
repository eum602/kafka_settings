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