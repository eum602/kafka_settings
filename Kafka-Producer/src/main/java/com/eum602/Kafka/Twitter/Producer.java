package com.eum602.Kafka.Twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.eum602.Kafka.Twitter.Env.*;

public class Producer {
    Logger logger = LoggerFactory.getLogger(Producer.class);

    String consumerKey =  CONSUMER_KEY;
    String consumerSecret = CONSUMER_SECRET;
    String token = TOKEN;
    String secret = SECRET;

    List<String> terms = Lists.newArrayList("kafka","ethereum"); //this is the search criteria

    public Producer() {
    }


    public static void main(String[] args) {
        new Producer().run();
    }

    public void run() {
        logger.info("Setup");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000); //capacity of 1000 messages
        //Create a twitter client
        Client client = createTwitterClient(msgQueue);
        client.connect(); // Attempts to establish a connection

        //Create a kafka producer
        KafkaProducer<String,String> producer = createKafkaProducer();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("stopping application...");
            logger.info("Shutting down client from twitter...");
            client.stop();
            logger.info("Closing producer...");
            producer.close(); //producer sends all messages it still have to kafka before shut down
            logger.info("done!");
        }));

        //Loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if (msg != null){
                logger.info(msg);
                producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {//setting a null key for now.
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null){
                            logger.error("Something bad happened: " + exception);
                        }
                    }
                });

            }
        }
        logger.info("End of application");

    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){
        /****************DECLARING THE CONNECTION INFORMATION******************/
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
            // Optional: set up some followings and track terms
        //List<Long> followings = Lists.newArrayList(1234L, 566788L);
        //hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

            // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret,token, secret);

        /******************************CREATING A CLIENT****************************/
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01") //created the client
                .hosts(hosebirdHosts) //connecting to the stream host
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    public KafkaProducer<String,String> createKafkaProducer(){
        String bootstrapServers =  "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties); //We want the key and value to be a string
        return  producer;
    }
}
