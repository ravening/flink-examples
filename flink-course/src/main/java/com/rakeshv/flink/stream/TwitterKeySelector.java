package com.rakeshv.flink.stream;

import org.apache.flink.api.java.functions.KeySelector;

public class TwitterKeySelector implements KeySelector<Tweet, String> {
    @Override
    public String getKey(Tweet value) throws Exception {
        return value.getLanguage();
    }
}
