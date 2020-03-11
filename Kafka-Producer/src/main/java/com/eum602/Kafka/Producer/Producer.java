package com.eum602.Kafka.Producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Producer.class);
        String bootstrapServers =  "127.0.0.1:9092";
        //Create producer properties
        Properties properties = new Properties();//creating a new properties object
        //https://kafka.apache.org/documentation/#producerconfigs
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());//key and value serializer helps the producer to know what type of value we are sending to kafka.
        //and how it should be serialized to bytes, because kafka will convert whatever we send to kafka into bytes(0 and 1's)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties); //We want the key and value to be a string

        //Create a producer record
        for (int i=0; i<10;i++){
            ProducerRecord<String,String> record =  new ProducerRecord<>("first_topic", "message #" + Integer.toString(i));
            //as we do not use any keys then all the messages will go to random partitions. What is more all messages will go to all partitions.

            //send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //executes every time a record is successfully sent or an exception is thrown
                    if (exception == null){
                        //success
                        logger.info("Received new metadata. \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    }else {
                        logger.error("Error while producing" + exception);
                    }
                }
            });
        }

        //flush code
        producer.flush(); //wait for the data to be produced
        //flush and close
        producer.close();
    }
}
