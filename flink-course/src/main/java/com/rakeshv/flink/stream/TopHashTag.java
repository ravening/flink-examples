package com.rakeshv.flink.stream;

import com.rakeshv.flink.sources.TwitterSourceCreator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Objects;

public class TopHashTag {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.addSource(TwitterSourceCreator.create())
                .map(new MapToTweets())
                .filter(Objects::nonNull)
                .filter(new FilterTweetsByLanguage())
                .flatMap(new FlatMapFunction<Tweet,
                        Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(Tweet tweet,
                                        Collector<Tuple2<String, Integer>> out) throws Exception {

                        for (var tag : tweet.getTags()) {
                            out.collect(new Tuple2<>(tag, 1));
                        }
                    }
                })
                .keyBy(new TwitterHashtagKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .sum(1)
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(5)))
                .apply(new AllWindowFunction<Tuple2<String, Integer>,
                        Tuple3<Date, String, Integer>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window,
                                      Iterable<Tuple2<String, Integer>> iterable,
                                      Collector<Tuple3<Date, String, Integer>> out) throws Exception {

                        String topTag = null;
                        int count = 0;

                        for (var hashTag : iterable) {
                            if (hashTag.f1 > count) {
                                topTag = hashTag.f0;
                                count = hashTag.f1;
                            }
                        }

                        out.collect(new Tuple3<>(new Date(window.getEnd()), topTag, count));
                    }
                })
                .print();

        env.execute("Top hashtags");
    }
}
