package com.rakeshv.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterStrings {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> dataStream = env
                .socketTextStream("localhost", 9999)
                .filter(new StringFilter())
                .map(new Roundup())
                ;

        dataStream.print();
        env.execute("Filter strings");
    }

    static class StringFilter implements FilterFunction<String> {

        @Override
        public boolean filter(String value) throws Exception {
            try {
                Double.parseDouble(value.trim());
                return true;
            } catch (Exception e) {}

            return false;
        }
    }

    static class Roundup implements MapFunction<String, Long> {

        @Override
        public Long map(String value) throws Exception {
            double d = Double.parseDouble(value.trim());

            return Math.round(d);
        }
    }
}
