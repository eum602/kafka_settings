package com.eum602.Kafka.Producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
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
        ProducerRecord<String,String> record =  new ProducerRecord<>("first_topic", "hello world");

        //send data
        producer.send(record);

        //flush code
        producer.flush(); //wait for the data to be produced
        //flush and close
        producer.close();
    }
}
