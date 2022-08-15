package com.rakeshv.flink.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.util.Properties;

public class FilterEnglishTweets {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty(TwitterSource.CONSUMER_KEY, "6DbmVK2Scd5VmJRU9wJbZ6Vux");
        properties.setProperty(TwitterSource.CONSUMER_SECRET, "uqUtLUerHUwDLBMBmxVGvR9EgnlEEOdzBnF75vF3Vhsaqy5yiq");
        properties.setProperty(TwitterSource.TOKEN, "171893480-8XUI7Usnvmp2057WYTTerfsgAUNgkgi2QGn7qCbT");
        properties.setProperty(TwitterSource.TOKEN_SECRET, "ms6eHe71Q1Azz8tMYx2TGStWvNUxkFBerarPS9QS0RqAY");

        env.addSource(new TwitterSource(properties))
                .map(new MapToTweets())
                .filter(new FilterFunction<Tweet>() {
                    @Override
                    public boolean filter(Tweet tweet) throws Exception {
                        return tweet.getLanguage().equalsIgnoreCase("en");
                    }
                })
                .print();

        env.execute("English Tweets");

    }
}
