package com.github.eum602.kafka_settings.consumers.ElasticSearchConsumer;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer {
    //https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-document-index.html
    public static RestHighLevelClient createClient(){
        /*This function creates a elastic search client*/
        String hostname = Env.HOSTNAME;
        String username = Env.USERNAME;
        String password = Env.PASSWORD;

        //the credentials provider (only because it is running on cloud, but not necessary for local elastic search)
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username,password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);//apply the credentials to https calls
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder); //client will allow to insert data into elastic
        return client;
    }

    public static void main(String[] args) throws IOException {
        new Consumer().run();
    }

    private void run(){
        Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-elastic-search"; //by changing this we will consume all content of the topics.
        String topic = "twitter_tweets";

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
        private Logger logger = LoggerFactory.getLogger(Consumer.class);
        private RestHighLevelClient client = createClient();//create a client connection to elk
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
                    //insert data into the processor of data (eg. elk, blockchain node , etc)
                    String jsonString = record.value();
                    IndexRequest indexRequest = new IndexRequest("twitter")
                            .source(jsonString, XContentType.JSON);
                    IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                    String id = indexResponse.getId();
                    logger.info(id);
                    try{
                        Thread.sleep(1000);
                    }catch (InterruptedException e){
                        e.printStackTrace();
                    }
                }
            }
            }catch (WakeupException e){
                logger.info("Received shutdown signal!");
            } catch (IOException e) {//because of the client.index
                e.printStackTrace();
            } finally {
                //close the consumer
                logger.info("closing the consumer...");
                consumer.close();
                //close connection to processor of messages (eg. elk, blockchain node, etc)
                try {
                    logger.info("closing connection to elastic-search");
                    client.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
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
