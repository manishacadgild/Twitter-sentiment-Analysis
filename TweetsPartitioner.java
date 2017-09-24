/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accenture.lkm.twiterfeedsimulator;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
/**
 *
 * @author rahul.suresh.ghali
 */
public class TweetsPartitioner implements Partitioner{
    private static final String HASHTAG1="#ICC";
    private static final String HASHTAG2="#BOLLYWOOD";
    
    public TweetsPartitioner() {
    
    }
    public TweetsPartitioner(VerifiableProperties props) {
    
    }
    
    @Override
    public int partition(Object o, int i) {
        String key = o.toString();
        if(key.equals(HASHTAG1))
            return 0;
        else if(key.equals(HASHTAG2))
            return 1;
        else
            return 2;
    }
    
}
