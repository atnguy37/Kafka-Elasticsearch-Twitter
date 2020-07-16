package com.github.simplesteph.kafka.tutorial3;

import com.google.gson.JsonParser;
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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.CredentialException;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient () {

        String hostname = "HOST_NAME"
        String username = "USER_NAME";
        String password = "PASSWORD";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient
                            (HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client =  new RestHighLevelClient(builder);
        return client;
    }


    private static JsonParser jsonParser = new JsonParser();
    private static String extractIDFromTweet(String jsonTweet) {
        //using Gson Library of Google
        return jsonParser.parse(jsonTweet)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
        //return "";
    }
    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        //create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");//disable auto commit to commit manually
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"20"); //Maximum of Records each poll
        // By default, this settings includes "At Least One" unless we set disable
        // in that case, when consumer read data from Kafka twice, which lead to duplicate when we insert them to
        // ElasticSearch.

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        RestHighLevelClient client = createClient();


        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            Integer recordCount = records.count();
            logger.info("Received " + recordCount + " records");

            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String,String> record : records) {
                //logger.info(record.value());

                try {
                    String id = extractIDFromTweet(record.value());



                    //it will fail unless index does exist
                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                                id // this is for idempotent
                    ).source(record.value(), XContentType.JSON);
                    //To avoid duplicate of data inserted to ElasticSearch, We need to include ID in IndexRequest
                    // To make it possible, we have two approach
                    // 1. Kafka Generic ID
                    // String id = record.topic() + "_" + record.partition() + "_" + record.offset()
                    // id above should be unique for each message in Kafka

                    //2. Use ID of Twitter Tweets Directly =>
                    // Exact id from json string of tweet

                    // this one is for one record per request insert to ElasticSearch
    //                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
    //                id = indexResponse.getId();
    //                logger.info(id);
    //                try {
    //                    Thread.sleep(10);
    //                } catch (InterruptedException e) {
    //                    e.printStackTrace();
    //                }

                    bulkRequest.add(indexRequest); //we add to our bulk request (takes no time)
                } catch (NullPointerException e) {
                    logger.warn("Skipping bad data: " + record.value());
                }

            }
            if (recordCount > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets...");
                consumer.commitSync(); //Sync, not Async
                logger.info("Offsets have been committed");
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //https://discuss.elastic.co/t/total-fields-limit-setting/53004
        }

        //close the client gracefully
        //client.close();

    }
}

//https://stackoverflow.com/questions/41216306/elasticsearch-statuslogger-log4j2-could-not-find-a-logging-implementation-plea/51988904
