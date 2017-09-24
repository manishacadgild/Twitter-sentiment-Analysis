/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accenture.lkm.twiterfeedsimulator;

import java.util.*;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 *
 * @author rahul.suresh.ghali
 */
public class TweetsProducer implements Runnable {

    private static final int MIN = 1;
    private static final int MAX = 7;
    private static final String TOPIC="TWEETS";
    private static final String HASHTAG1="#ICC";
    private static final String HASHTAG2="#BOLLYWOOD";
    private static final String SPACE = " ";
    private ProducerConfig config;
    private Producer<String, String> producer; 

    public TweetsProducer() {
        config=this.createProducerConfig();
        producer = new Producer<String, String>(config);    
    }
    
    
    @Override
    public void run() {
        int i = 0;
        boolean fail = false;
        while (i < 10000) {
            if (!fail) {
                i++;
            }
            String tweet = createTweet();
            System.out.println(tweet);
            String key = tweet.split(SPACE)[0];
            KeyedMessage<String, String> kafkaMessage = new KeyedMessage<String, String>(TOPIC,key, tweet);
            try {
                System.out.println(kafkaMessage.message());
                producer.send(kafkaMessage);                
                Thread.sleep(10);
                fail = false;
            } catch (InterruptedException ex) {
                System.out.println("Thread interupted.. ");
            } catch (Exception ex) {
                System.out.println("Exception while publishing a message");
                fail = true;
            }
        }
    }

    private ProducerConfig createProducerConfig() {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "com.accenture.lkm.twiterfeedsimulator.TweetsPartitioner");
        props.put("request.required.acks", "1");
        props.put("request.timeout.ms", "30000");
        props.put("message.send.max.retries", "5");
        return new ProducerConfig(props);
    }

    private String createTweet() {
        Random r = new Random();
        int random = r.nextInt((MAX - MIN) + 1) + MIN;
        StringBuilder tweetBuilder = new StringBuilder();        
        switch (random) {
            case 1:
                tweetBuilder.append(HASHTAG1+SPACE+"Virat");
                break;
            case 2:
                tweetBuilder.append(HASHTAG1+SPACE+"Simmons");
                break;
            case 3:
                tweetBuilder.append(HASHTAG1+SPACE+"Gayle");
                break;
            case 4:
                tweetBuilder.append(HASHTAG1+SPACE+"Dhoni");
                break;
            case 5:
                tweetBuilder.append(HASHTAG1+SPACE+"Rohit");
                break;
            case 6:
                tweetBuilder.append(HASHTAG2+SPACE+"Deepika");
                break;
            case 7:
                tweetBuilder.append(HASHTAG2+SPACE+"Ranveer");
                break;
        }
        return tweetBuilder.toString();
    }
}
