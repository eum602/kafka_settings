package Kafka.Basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        System.out.println("Starting the Consumer");
        Logger logger = LoggerFactory.getLogger(Consumer.class);
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "My-application";
        String topic = "first_topic";

        //by running this we can see if the consumer is already up to date with the messages from kafka or it there is a lag.
        //kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group My-application
        /*If you want to read again from the beggining you can either have to reset the groupID or you just can change the groupId name.*/

        //Create consumer configs
        Properties properties = new Properties(); //https://kafka.apache.org/documentation/#consumerconfigs
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        /*The producer takes a string, serializes it (to byres) and sends it to kafka; then Kafka sends it to the consumer which deserializes it-*/
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        /*
        * "earliest" ==> Means you want to read from the very beginning of your topic
        * "latest" ==> Means the consumer reads from the only latest messages
        * "none" ==> if set to "none" it will throw an error if there are no offsets being saved.
        * Usually "earliest" or "latest" are used
        * */

        //create the kafka consumer
        KafkaConsumer<String,String> consumer =  new KafkaConsumer<String, String>(properties);

        //subscribe a consumer to a topic(s)
        //consumer.subscribe(Collections.singleton(topic));//consumer can take a topic or a collection of topics
        //By doing a collection.singleton we are only subscribing to one topic; but you could definitely subscribe to only one topic.

        //to subscribe to more than one topic we can:
        //consumer.subscribe(Arrays.asList("first_topic","second_topic","n_topic"));
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data
        while (true){
            ConsumerRecords<String,String> records =  consumer.poll(Duration.ofMillis(100));//timeout of 100 milliseconds

            //looking into the records
            for (ConsumerRecord<String,String> record : records ){
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }
}
