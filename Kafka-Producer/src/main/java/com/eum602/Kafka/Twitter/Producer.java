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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Producer {
    Logger logger = LoggerFactory.getLogger(Producer.class);

    String consumerKey =  "9Rrc2eupWoKTPdqf8rhWrAphl";
    String consumerSecret = "aQMhGVSMcGEPHoUEqFpFVwZrd7Bmo4OnTa7voye6pNNLfyHnme";
    String token = "1799023009-hm9ua0aX2AtJZyQXVap4X5fvIsj9Z4GSpETvRMJ";
    String secret = "sBeLo2E0sUClGhrqPtaTDV5dwfxZgSewW1fMaSCJfbQH8";

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
        List<String> terms = Lists.newArrayList("bitcoin"); //this is the search criteria
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
}
