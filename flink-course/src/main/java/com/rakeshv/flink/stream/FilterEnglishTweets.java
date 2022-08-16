package com.rakeshv.flink.stream;

import com.rakeshv.flink.sources.TwitterSourceCreator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

public class FilterEnglishTweets {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(TwitterSourceCreator.create())
                .map(new MapToTweets())
                .filter(Objects::nonNull)
                .filter(new FilterTweetsByLanguage())
                .print();

        env.execute("English Tweets");

    }
}
