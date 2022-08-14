package com.rakeshv.flink;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;

public class AverageGenreRating {
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

        filename = fileReader.getFilePath("ratings.csv");

        DataSet<Tuple2<Integer, Double>> ratings =
                env.readCsvFile(filename)
                        .ignoreFirstLine()
                        .ignoreInvalidLines()
                        .includeFields(false, true, true, false)
                        .types(Integer.class, Double.class);

        // join them
        List<Tuple2<String, Double>> genreRating = movies.join(ratings)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple3<Integer, String, String>, Tuple2<Integer, Double>,
                        Tuple3<StringValue, StringValue, DoubleValue>>() {
                    private StringValue name = new StringValue();
                    private StringValue genre = new StringValue();
                    private DoubleValue score = new DoubleValue();

                    private Tuple3<StringValue, StringValue, DoubleValue> result
                            = new Tuple3<>(name, genre, score);
                    @Override
                    public Tuple3<StringValue, StringValue, DoubleValue> join(Tuple3<Integer, String, String> movie, Tuple2<Integer, Double> rating) throws Exception {
                        name.setValue(movie.f1);
                        genre.setValue(movie.f2.split("\\|")[0]);
                        score.setValue(rating.f1);
                        return result;
                    }
                })
                .groupBy(1)
                .reduceGroup(new GroupReduceFunction<Tuple3<StringValue, StringValue, DoubleValue>, Tuple2<String, Double>>() {
                    @Override
                    public void reduce(Iterable<Tuple3<StringValue, StringValue, DoubleValue>> iterable,
                                       Collector<Tuple2<String, Double>> collector) throws Exception {
                        String genre = null;
                        int count = 0;
                        double totalScore = 0;

                        for (var movie : iterable) {
                            genre = movie.f1.getValue();
                            totalScore += movie.f2.getValue();
                            count++;
                        }

                        collector.collect(new Tuple2<>(genre, totalScore / count));
                    }
                })
                .collect();


        String result = genreRating.stream()
                .sorted((r1, r2) -> Double.compare(r1.f1, r2.f1))
                .map(Object::toString)
                .collect(Collectors.joining("\n"));

        System.out.println(result);

    }
}
