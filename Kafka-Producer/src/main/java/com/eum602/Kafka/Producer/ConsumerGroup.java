package com.eum602.Kafka.Producer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerGroup {
    public static void main(String[] args) {
        new ConsumerGroup().run();
    }

    private ConsumerGroup(){}

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerGroup.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "My-consumer-application-with-threads"; //by changing this we will consume all content of the topics.
        String topic = "first_topic";

        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //creates the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerThread(bootstrapServers,groupId,topic,latch);

        //Start the thread
        Thread myThread =  new Thread(myConsumerRunnable);
        myThread.start();

        //add a shutdown hook ==> it properly shutdown the application
        Runtime.getRuntime().addShutdownHook(new Thread( () -> { //creating a new function with lambda functions syntax
            logger.info("Caught shutdown hook");
            ((ConsumerThread) myConsumerRunnable).shutDown();

            try {//add the latch.await() to properly shutdown
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }

        ));


        try {
            latch.await();//waits all the way until the application is over
        } catch (InterruptedException e) {
            logger.error("Application got interrupted",e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerThread implements Runnable{ //In this consumer thread, the consumer will perform the consuming operation
        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerGroup.class);
        public ConsumerThread(
                String bootstrapServers,
                String groupId,
                String topic,
                CountDownLatch latch) {//CountDown is something in Java which helps us deal with concurrency
            this.latch = latch; //this latch is gonna be able to shutdown an application correctly
            //create the kafka consumer

            //Create consumer configs
            Properties properties = new Properties(); //https://kafka.apache.org/documentation/#consumerconfigs
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            /*The producer takes a string, serializes it (to byres) and sends it to kafka; then Kafka sends it to the consumer which deserializes it-*/
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); //earliest is equivalent to the "from-beginning" option to the CLI

            consumer =  new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            //poll for new data
            try{while (true){
                    ConsumerRecords<String,String> records =  consumer.poll(Duration.ofMillis(100));//timeout of 100 milliseconds

                    //looking into the records
                    for (ConsumerRecord<String,String> record : records ){
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            }catch (WakeupException e){
                logger.info("Received shutdown signal!");
            }finally {
                //close the consumer
                consumer.close();
                //tell our main code we are done with the consumer
                latch.countDown();
            }
        }

        public void shutDown() {
            //wakeup() method is made to interrupt consumer.poll(), it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}
