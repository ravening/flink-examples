package org.apache.flink.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.LocalDateTime;
import java.util.Random;

public class RandomString implements SourceFunction<String> {

    Random random = new Random();
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        for (var i = 1; i < 1000; i++) {
            int id = i;
            LocalDateTime timeStamp = LocalDateTime.now();

            String message = String.format("ID: %d, Timestamp: %s", i, timeStamp.toString());
            sourceContext.collect(message);
            try {
                Thread.sleep(random.nextInt(2000));
            } catch (Exception e) {

            }
        }
    }

    @Override
    public void cancel() {

    }
}
