package com.rakeshv.flink.stream;

import com.rakeshv.flink.utils.StreamUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class ViewsSessionWindow {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

        if (dataStream == null) {
            System.exit(1);
            return;
        }

        DataStream<Tuple2<String, Double>> averageViewStream = dataStream
                .map(new RowSplitter())
                .map(new PerUser())
                .keyBy(0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                //.max(2)
                .reduce(new Sum());

        averageViewStream.print();

        env.execute("Average Views Per User and Per User, Per Page");
    }


    public static class RowSplitter implements
            MapFunction<String, Tuple3<String, String, Double>> {

        public Tuple3<String, String, Double> map(String row)
                throws Exception {
            String[] fields = row.split(",");
            if (fields.length == 3) {
                return new Tuple3<String, String, Double>(
                        fields[0] /* user id */,
                        fields[1] /* webpage id */,
                        Double.parseDouble(fields[2]) /* view time in minutes */);
            }

            return null;
        }
    }

    public static class PerUser implements
            MapFunction<Tuple3<String, String, Double>, Tuple2<String, Double>> {

        public Tuple2<String, Double> map(Tuple3<String, String, Double> input) {
            return new Tuple2<String, Double>(
                    (String) input.getField(0),
                    (Double) input.getField(2));
        }
    }

    public static class Sum implements ReduceFunction<Tuple2<String, Double>> {
        public Tuple2<String, Double> reduce(
                Tuple2<String, Double> cumulative,
                Tuple2<String, Double> input) {
            return new Tuple2<String, Double>(
                    (String) cumulative.getField(0),
                    (Double)cumulative.getField(1) + (Double) input.getField(1));
        }
    }
}








