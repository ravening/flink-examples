package com.rakeshv.flink.stream;

import com.rakeshv.flink.utils.StreamUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountCountWindow {
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

        DataStream<WordCount> wordCountStream = dataStream
                .flatMap(new CountWindowSplitter())
                .keyBy("word")
                .countWindow(3)
                .sum("count");

        wordCountStream.print();

        env.execute("Count window");
    }

    static class CountWindowSplitter implements
            FlatMapFunction<String, WordCount> {

        @Override
        public void flatMap(String sentence, Collector<WordCount> out)
                throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new WordCount(word, 1));
            }
        }
    }

    public static class WordCount {
        public String word;
        public Integer count;

        public WordCount() {
        }

        public WordCount(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        public String toString() {
            return word + ": " + count;
        }
    }
}
