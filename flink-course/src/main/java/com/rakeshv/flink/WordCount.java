package com.rakeshv.flink;

import com.rakeshv.flink.functions.WordCountSplitter;
import com.rakeshv.flink.utils.StreamUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCount {

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
                .sum(1);

        wordCountStream.print();

        env.execute("Word count splitter");
    }
}
