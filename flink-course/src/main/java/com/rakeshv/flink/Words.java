package com.rakeshv.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Words {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream = null;

        if (params.has("input")) {
            System.out.println("Executing program with file input");
            dataStream = env.readTextFile(params.get("input"));
        } else if (params.has("host") && params.has("port")) {
            System.out.println("Executing program by reading from socket stream");
            dataStream = env.socketTextStream(params.get("host"), params.getInt("port"));
        } else {
            System.err.println("Either pass --input to read from file or pass both --host and --port");
            System.exit(1);
        }

        dataStream.flatMap(new Splitter())
                .print();

        env.execute("Flatmap example");
    }

    public static class Splitter implements FlatMapFunction<String, String> {

        public void flatMap(String sentence, Collector<String> out)
                throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(word);
            }
        }
    }
}
