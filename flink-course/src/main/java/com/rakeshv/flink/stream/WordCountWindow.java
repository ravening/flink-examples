package com.rakeshv.flink.stream;

import com.rakeshv.flink.functions.WordCountSplitter;
import com.rakeshv.flink.utils.StreamUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WordCountWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);
        if (dataStream == null) {
            System.exit(1);
            return;
        }

        DataStream<Tuple2<String, Integer>> wordCountStream = dataStream
                .flatMap(new WordCountSplitter())
                .keyBy(0)
                // for tumbling window without overlapping
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))

                // for sliding window with overlapping
                .window(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .sum(1);

        wordCountStream.print();
        env.execute("Word count timing and sliding window");
    }
}
