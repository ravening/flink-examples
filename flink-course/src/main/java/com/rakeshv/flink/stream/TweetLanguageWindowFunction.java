package com.rakeshv.flink.stream;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple3;

import java.util.Date;

public class TweetLanguageWindowFunction
        implements WindowFunction<Tweet,
        Tuple3<String, Long, Date>, String, TimeWindow> {
    @Override
    public void apply(String language,
                      TimeWindow window,
                      Iterable<Tweet> input,
                      Collector<Tuple3<String, Long, Date>> out) throws Exception {
        long count = 0;
        for (Tweet tweet : input) {
            count++;
        }

        out.collect(new Tuple3<>(language, count, new Date(window.getEnd())));
    }
}
