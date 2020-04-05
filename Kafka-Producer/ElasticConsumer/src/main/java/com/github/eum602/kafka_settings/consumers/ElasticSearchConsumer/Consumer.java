package com.github.eum602.kafka_settings.consumers.ElasticSearchConsumer;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
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
        Logger logger = LoggerFactory.getLogger(Consumer.class);
        RestHighLevelClient client = createClient();
        String jsonString =  "{\"foo\":\"bar\"}";
        IndexRequest indexRequest = new IndexRequest(
                "twitter")
                .source(jsonString, XContentType.JSON); //putting some data into
        //twitter tweets (it has to be created in elastic => /_cat/indices?v 'to create twitter index')
        //adding the client to run the code

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        logger.info(id);
        client.close();
    }
}
