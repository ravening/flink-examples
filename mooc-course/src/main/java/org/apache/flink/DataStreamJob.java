/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.models.SensorData;
import org.apache.flink.sources.RandomString;
import org.apache.flink.sources.SensorDataSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.fromSequence(1, 10);
		 *
		 * then, transform the resulting DataStream<Long> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.window()
		 * 	.process()
		 *
		 * and many more.
		 * Have a look at the programming guide:
		 *
		 * https://nightlies.apache.org/flink/flink-docs-stable/
		 *
		 */

		DataStream<String> inputDataStream =
				env.addSource(new RandomString());


//		inputDataStream.process(new ProcessFunction<String, String>() {
//
//			@Override
//			public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
//				System.out.println(s);
//				collector.collect(s);
//			}
//		});

		// Execute program, beginning computation.
//		env.execute(DataStreamJob.class.getName());

		DataStream<String> sensorDataSource = env.addSource(new SensorDataSourceFunction());

		// one way of mapping
//		DataStream<SensorData> sensorDataStream = sensorDataSource.map(new MapFunction<String, SensorData>() {
//			@Override
//			public SensorData map(String s) throws Exception {
//				return new Gson().fromJson(s, SensorData.class);
//			}
//		});

		// second way of mapping
		DataStream<SensorData> sensorDataStream = sensorDataSource
				.map((MapFunction<String, SensorData>) s -> new Gson().fromJson(s, SensorData.class));

		sensorDataStream.process(new ProcessFunction<SensorData, String>() {

			@Override
			public void processElement(SensorData sensorData, ProcessFunction<SensorData, String>.Context context, Collector<String> collector) throws Exception {
				System.out.println(sensorData);
				collector.collect(sensorData.toString());
			}
		});
//		sensorDataSource.process(new ProcessFunction<String, String>() {
//			@Override
//			public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
//				System.out.println(s);
//				collector.collect(s);
//			}
//		});
		env.execute(SensorDataSourceFunction.class.getName());
	}
}
