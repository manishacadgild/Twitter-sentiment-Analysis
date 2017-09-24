/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accenture.lkm.twiterfeedsimulator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author rahul.suresh.ghali
 */
public class Driver {

    public static void main(String[] args) {
        System.out.println("Start Twitter service............");
        ExecutorService tweetExecutorService = Executors.newFixedThreadPool(5);
        tweetExecutorService.submit(new TweetsProducer());
        tweetExecutorService.submit(new TweetsProducer());
        tweetExecutorService.submit(new TweetsProducer());
        tweetExecutorService.submit(new TweetsProducer());
        tweetExecutorService.submit(new TweetsProducer());
        tweetExecutorService.shutdown();
        while (!tweetExecutorService.isTerminated()) {

        }
        System.out.println("Stopping Twitter service............");
        
    }
}
