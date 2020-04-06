# Kafka - configuration #

## Install Kafka with ansible
* Link <a href="https://github.com/eum602/Installer"> here </a>

## Useful monitoring tool
*  Monitoring tool <a href="https://github.com/yahoo/kafka-manager"> here </a>

Before proceeding make sure you have started zookeeper and kafka, also make sure you have created necessary topics (follow the scrips folder <a href="https://github.com/eum602/kafka_settings/tree/master/scripts/1_topics"> here </a>)
## Running kafka producer
To run the kafka producer use this <a href="https://github.com/eum602/kafka_settings/blob/master/Kafka-Producer/src/main/java/com/eum602/Kafka/Producer/Producer.java"> file </a>
## Running kafka consumer
To run the kafka consumer use this <a href="https://github.com/eum602/kafka_settings/blob/master/Kafka-Producer/src/main/java/com/eum602/Kafka/Producer/Consumer.java"> file </a>

## Common Issues
If using ElasticSearch with bonsai make sure to set an appropriate number of requests:
```shell
PUT twitter/_settings
{
  "index.mapping.total_fields.limit": 5000
}
```
