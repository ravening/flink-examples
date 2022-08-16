package com.rakeshv.flink.stream;

import com.rakeshv.flink.sources.TwitterSourceCreator;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Objects;

public class StreamTweetsByLanguage {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.addSource(TwitterSourceCreator.create())
                .map(new MapToTweets())
                .filter(Objects::nonNull)
                .keyBy(new TwitterKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply(new TweetLanguageWindowFunction())
                .print();
//                .timeWindow(Time.minutes(1))

        env.execute("Tweets by language");
    }
}
