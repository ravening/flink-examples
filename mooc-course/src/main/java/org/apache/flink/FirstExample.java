package org.apache.flink;

import com.google.gson.Gson;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.models.SensorData;
import org.apache.flink.sources.SensorDataSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class FirstExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> sensorDataSource = env.addSource(new SensorDataSourceFunction());

        DataStream<SensorData> sensorDataStream = sensorDataSource
                .map((MapFunction<String, SensorData>) s -> new Gson().fromJson(s, SensorData.class))
                .assignTimestampsAndWatermarks(new WatermarkStrategy<SensorData>() {
                    @Override
                    public WatermarkGenerator<SensorData> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<SensorData>() {
                            @Override
                            public void onEvent(SensorData sensorData, long l, WatermarkOutput watermarkOutput) {
                                watermarkOutput.emitWatermark(new Watermark(sensorData.getTimestamp()));
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

                            }
                        };
                    }

                    @Override
                    public TimestampAssigner<SensorData> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                        return new TimestampAssigner<SensorData>() {
                            @Override
                            public long extractTimestamp(SensorData sensorData, long l) {
                                return sensorData.getTimestamp();
                            }
                        };
                    }
                })
                ;

        DataStream<SensorData> sensorDataStreamFlatMapped = sensorDataStream
                .filter(new FilterFunction<SensorData>() {
                    @Override
                    public boolean filter(SensorData sensorData) throws Exception {
                        long sensorId = sensorData.getSensorId();

                        return sensorId == 10L || sensorId == 19L ||
                                sensorId == 29L || sensorId == 1L ||
                                sensorId == 3L || sensorId == 5L ||
                                sensorId == 101L || sensorId == 20L ||
                                sensorId == 40L || sensorId == 30L;
                    }
                })
                .flatMap(new FlatMapFunction<SensorData, SensorData>() {
                    @Override
                    public void flatMap(SensorData sensorData, Collector<SensorData> collector) throws Exception {
                        long sensorId = sensorData.getSensorId();

                        SensorData sensorData1 = new SensorData(sensorData.getSensorType(),
                                sensorData.getValue(), sensorId, sensorData.getTimestamp());
                        collector.collect(sensorData1);
                    }
                });

        KeyedStream<SensorData, String> keyedStream = sensorDataStream
                .keyBy(SensorData::getSensorType);

        keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                ;

//        sensorDataStreamFlatMapped.print();

        env.execute("first example");

    }
}
