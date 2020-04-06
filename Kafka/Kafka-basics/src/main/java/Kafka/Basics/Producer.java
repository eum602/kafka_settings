package Kafka.Basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
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

        for (int i=0; i<10;i++){
            //Create a producer record
            String topic = "first_topic";
            String value = "message #" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            ProducerRecord<String,String> record =  new ProducerRecord<>(topic, key, value); // ==> the same keys goes to the same partition when a key us assigned
            //as we do not use any keys then all the messages will go to random partitions. What is more all messages will go to all partitions.

            logger.info("Key:" + key);

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
            }).get(); //only for demo NOT for production ==>write this ".get" then press alt + enter ==> which will give you the opportunity to add an exception to this function
            //what ".get()" does is to clock the .send() method in order to make this function synchronous.
        }
        /*
         * id_0 => partition 1
         * id_1 => partition 0
         * id_2 => partition 2
         * id_3 => partition 0
         * ... ==> this order will be deterministic because when using keys each key is always sent to the same partition.
         * */

        //flush code
        producer.flush(); //wait for the data to be produced
        //flush and close
        producer.close();
    }
}
