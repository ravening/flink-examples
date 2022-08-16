package com.rakeshv.flink.stream;

import org.apache.flink.api.common.functions.FilterFunction;

public class FilterTweetsByLanguage implements FilterFunction<Tweet> {
    @Override
    public boolean filter(Tweet tweet) throws Exception {
        return tweet.getLanguage().equalsIgnoreCase("en");
    }
}
