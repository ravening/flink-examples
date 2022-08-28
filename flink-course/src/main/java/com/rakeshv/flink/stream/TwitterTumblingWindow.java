package com.rakeshv.flink.stream;

import com.rakeshv.flink.sources.TwitterSourceCreator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class TwitterTumblingWindow {

    public static void main(String[] args) throws Exception {

//        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

//        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> streamSource = env.addSource(TwitterSourceCreator.create());

        DataStream<Tuple2<String, Integer>> tweets = streamSource
                .flatMap(new LanguageCount())
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1);

        tweets.print();

        env.execute("Twitter Language Count");
    }

    public static class LanguageCount
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        private transient ObjectMapper jsonParser;

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out)
                throws Exception {

            if(jsonParser == null) {
                jsonParser = new ObjectMapper();
            }

            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

            String language = jsonNode.has("lang")
                    ? jsonNode.get("lang").asText()
                    : "unknown";

            out.collect(new Tuple2<>(language, 1));
        }
    }
}











