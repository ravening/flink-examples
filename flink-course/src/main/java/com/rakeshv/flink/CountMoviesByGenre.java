package com.rakeshv.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class CountMoviesByGenre {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        FileReader fileReader = new FileReader();
        String filename = fileReader.getFilePath("movies.csv");

        // read the file
        DataSet<Tuple3<Integer, String, String>> movies = env.readCsvFile(filename)
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .types(Integer.class, String.class, String.class);

        List<Tuple2<String, Integer>> genreCount = movies.flatMap(
                new FlatMapFunction<Tuple3<Integer, String, String>,
                        Tuple2<String, String>>() {
                    @Override
                    public void flatMap(Tuple3<Integer, String, String> csvLine,
                                        Collector<Tuple2<String, String>> collector) throws Exception {

                        String movieName = csvLine.f1;
                        String[] genres = csvLine.f2.split("\\|");
                        Arrays.stream(genres)
                                .forEach(genre -> collector.collect(new Tuple2<>(genre, movieName)));
                    }
                })
                .groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple2<String, String>,
                        Tuple2<String, Integer>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<String, String>> iterable,
                                       Collector<Tuple2<String, Integer>> collector) throws Exception {
                        int count = 0;
                        String genre = null;
                        for (var entry : iterable) {
                            genre = entry.f0;
                            count++;
                        }

                        collector.collect(new Tuple2<>(genre, count));
                    }
                }).collect();

        genreCount.parallelStream()
                .forEach(System.out::println);
    }
}
