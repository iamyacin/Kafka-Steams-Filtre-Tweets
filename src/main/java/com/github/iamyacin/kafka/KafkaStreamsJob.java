package com.github.iamyacin.kafka;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.*;


public class KafkaStreamsJob {

    static final JsonParser jsonParser = new JsonParser();

    static Integer getFollowersCount (String tweet){
        try {
            return jsonParser.parse(tweet).getAsJsonObject().get("payload").getAsJsonObject().get("User").getAsJsonObject().get("FollowersCount").getAsInt();
        } catch (NullPointerException e){ return 0; }
    }

    static String extractIdFromTweet(String tweet){
        return jsonParser.parse(tweet).getAsJsonObject().get("payload").getAsJsonObject().get("Id").getAsString();
    }

    static Object processTweet (Object tweet) {
        if (getFollowersCount((String) tweet)>4999) {
            JsonObject rawTweet = jsonParser.parse((String) tweet).getAsJsonObject().get("payload").getAsJsonObject();
            String tweetText = rawTweet.get("Text").getAsString();
            try {
                if (getTextClass(tweetText)==1){
                    String polarity=null;
                    int polar = getPolarity(tweetText);
                    if(polar == 1) polarity = "Positive";
                    if(polar == 0) polarity = "Neutral";
                    if(polar == -1) polarity = "Negative";
                    Date tweetDate = new Date(rawTweet.get("CreatedAt").getAsLong());
                    String tweetUser = "@"+rawTweet.get("User").getAsJsonObject().get("ScreenName").getAsString();
                    Boolean isRetweet = rawTweet.get("Retweet").getAsBoolean();
                    JsonArray hashtags = new JsonArray();
                    JsonArray symbols = new JsonArray();
                    JsonArray mentions = new JsonArray();
                    rawTweet.get("HashtagEntities").getAsJsonArray().forEach(obj -> hashtags.add(obj.getAsJsonObject().get("Text").getAsString()));
                    rawTweet.get("SymbolEntities").getAsJsonArray().forEach(obj -> symbols.add(obj.getAsJsonObject().get("Text").getAsString()));
                    rawTweet.get("UserMentionEntities").getAsJsonArray().forEach(obj -> mentions.add("@"+obj.getAsJsonObject().get("ScreenName").getAsString()));
                    JsonObject tweetObject = new JsonObject();
                    tweetObject.addProperty("Created At", String.valueOf(tweetDate));
                    tweetObject.addProperty("User", tweetUser);
                    tweetObject.addProperty("Followers Count", getFollowersCount((String) tweet));
                    tweetObject.addProperty("Text", tweetText);
                    tweetObject.add("Hashtags", hashtags);
                    tweetObject.add("Crypto Symbols", symbols);
                    tweetObject.add("Users Mentions", mentions);
                    tweetObject.addProperty("is Retweet", isRetweet);
                    tweetObject.addProperty("Polarity", polarity);
                    return tweetObject;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    static Integer getPolarity(String text) throws Exception {

        HttpPost post = new HttpPost("http://localhost:5000/Polarity");
        List<NameValuePair> urlParameters = new ArrayList<>();
        urlParameters.add(new BasicNameValuePair("text", text));

        post.setEntity(new UrlEncodedFormEntity(urlParameters));

        try (CloseableHttpClient httpClient = HttpClients.createDefault();
             CloseableHttpResponse response = httpClient.execute(post)) {
            return Integer.parseInt(EntityUtils.toString(response.getEntity()).trim());
        }


    }

    static Integer getTextClass(String text) throws Exception {

        HttpPost post = new HttpPost("http://localhost:5000/NewsOrNot");
        List<NameValuePair> urlParameters = new ArrayList<>();
        urlParameters.add(new BasicNameValuePair("text", text));

        post.setEntity(new UrlEncodedFormEntity(urlParameters));

        try (CloseableHttpClient httpClient = HttpClients.createDefault();
             CloseableHttpResponse response = httpClient.execute(post)) {
            return Integer.parseInt(EntityUtils.toString(response.getEntity()).trim());
        }
    }

    //method to create elasticsearch client
    static RestHighLevelClient createClient(){
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        return new RestHighLevelClient(builder);
    }

    public static void main(String[] args) {

        //create the elasticsearch client
        RestHighLevelClient client = createClient();

        //Creating the properties:
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "crypto-tweets-analysis");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();


        //input topic:
        KStream<String, String> inputTopic = streamsBuilder.stream("crypto-tweets");

        BulkRequest bulkRequest = new BulkRequest();
        inputTopic.foreach((key, value) -> {
            Object processedTweet = processTweet(value);
            if (processedTweet != null) {
                System.out.println(processedTweet);
                String id = extractIdFromTweet(value);
                //create the request that will send to elastic search (id as param to ignore insertion of duplicate records)
                try {
                    IndexRequest indexRequest = new IndexRequest("crypto-tweets", "tweet", id).source(processedTweet.toString(), XContentType.JSON);
                    bulkRequest.add(indexRequest); // add records into bulk request
                    client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    System.out.println("successfully Inserted to ElasticSearch");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        //build the topology
        streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.start();
    }
}
