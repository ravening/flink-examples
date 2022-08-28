package com.rakeshv.flink;

import com.rakeshv.flink.utils.StreamUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

public class AverageViews {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);
        if (dataStream == null) {
            System.exit(1);
            return;
        }

        DataStream<Tuple2<String, Double>> averageViewStream = dataStream
                .map(new RowSplitter())
                .filter(Objects::nonNull)
                .keyBy(0)
                .reduce(new SumAndCount())
                .map(new Average())
                ;

        averageViewStream.print();
        env.execute("Average views");
    }

    static class RowSplitter implements MapFunction<String, Tuple3<String, Double, Integer>> {

        @Override
        public Tuple3<String, Double, Integer> map(String value) throws Exception {
            String[] fields = value.split(",");
            if (fields.length == 2) {
                return new Tuple3<>(fields[0],
                        Double.parseDouble(fields[1]),
                        1);
            }

            return null;
        }
    }

    static class SumAndCount implements ReduceFunction<Tuple3<String, Double, Integer>> {
        @Override
        public Tuple3<String, Double, Integer>
        reduce(Tuple3<String, Double, Integer> value1,
               Tuple3<String, Double, Integer> value2) throws Exception {
            return new Tuple3<>(
                    value1.f0,
                    value1.f1 + value2.f1,
                    value1.f2 + value2.f2
            );
        }
    }

    static class Average implements MapFunction<Tuple3<String, Double, Integer>, Tuple2<String, Double>>{
        @Override
        public Tuple2<String, Double> map(Tuple3<String, Double, Integer> value) throws Exception {
            return new Tuple2<>(value.f0, (value.f1 / value.f2));
        }
    }
}
