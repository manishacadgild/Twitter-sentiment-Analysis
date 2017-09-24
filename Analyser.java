/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accenture.trend;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;


public class Analyser {

    /**
     * @param args the command line arguments
     */
    private static final String brokers = "localhost:9092";
    private static final String topics = "TWEETS";
    private static final String HASHTAG1 = "#ICCWCT20";

    public static void main(String[] args) {
        // TODO code application logic here

        SparkConf conf = new SparkConf().setAppName("Tweet Analyzer App");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);
        
        //Create a Discretized stream from a Kafka Topic
        JavaPairInputDStream<String, String> tweetStream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        //filter tweets with ICCWCT20
        JavaPairDStream<String, String> filteredTweetStream
                = tweetStream.filter(new Function<Tuple2<String, String>, Boolean>() {

                    @Override
                    public Boolean call(Tuple2<String, String> t1) throws Exception {
                        if (t1._2().contains(HASHTAG1)) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                });

        //extract the tweet
        JavaDStream<String> tweets = filteredTweetStream.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                //System.out.println("Message " + tuple2._2());
                return tuple2._2();
            }
        });
        //create pairs for the trending players
        JavaPairDStream<String, Integer> playerCount = tweets.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String x) throws Exception {
                        String trendingPlayer = x.split(" ")[1];
                        //System.out.println(trendingPlayer);
                        return new Tuple2<String, Integer>(trendingPlayer, 1);
                    }
                }
        );

        //create pair with player name and counts
        JavaPairDStream<String, Integer> trendingPlayerCount = playerCount.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) throws Exception {
                        return i1 + i2;
                    }
                }
        );

        System.out.println("Trending players in the last 5 minutes .... ");
        trendingPlayerCount.print();
        jssc.start();
        jssc.awaitTermination();
    }

}
