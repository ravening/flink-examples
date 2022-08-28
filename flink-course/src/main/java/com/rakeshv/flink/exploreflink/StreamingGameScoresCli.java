package com.rakeshv.flink.exploreflink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingGameScoresCli {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        if (!params.has("host") || !params.has("port")) {
            System.out.println("Please specify values for --host and --port");

            System.exit(1);
            return;
        }

        DataStream<String> dataStream = env.socketTextStream(
                params.get("host"), Integer.parseInt(params.get("port")));

        DataStream<Tuple2<String, Integer>> gameScores = dataStream
                .map(new ExtractPlayersAndScoresFn())
                .filter(new FilterPlayersAboveThresholdFn(100));

        gameScores.print();

        env.execute("Streaming game scores");
    }

    public static class ExtractPlayersAndScoresFn implements MapFunction<String, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(String s) throws Exception {
            String[] tokens = s.split(",");

            return Tuple2.of(tokens[0].trim(), Integer.parseInt(tokens[1].trim()));
        }
    }

    public static class FilterPlayersAboveThresholdFn implements FilterFunction<Tuple2<String, Integer>> {

        private int scoreThreshold = 0;

        public FilterPlayersAboveThresholdFn(int scoreThreshold) {
            this.scoreThreshold = scoreThreshold;
        }

        @Override
        public boolean filter(Tuple2<String, Integer> playersScores) throws Exception {
            return playersScores.f1 > scoreThreshold;
        }
    }
}
