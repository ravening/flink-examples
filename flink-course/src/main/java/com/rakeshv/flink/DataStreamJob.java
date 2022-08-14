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

package com.rakeshv.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;

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
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		FileReader fileReader = new FileReader();
		String filename = fileReader.getFilePath("movies.csv");

		// Batch processing
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// read the file

		DataSet<Tuple3<Integer, String, String>> lines = env.readCsvFile(filename)
				.ignoreFirstLine()
				.parseQuotedStrings('"')
				.ignoreInvalidLines()
				.types(Integer.class, String.class, String.class);

		lines.map((MapFunction<Tuple3<Integer, String, String>, Movie>) csvLine -> {
			int moveId = csvLine.f0;
			String movieName = csvLine.f1;
			String[] genres = csvLine.f2.split("\\|");

			return new Movie(moveId, movieName, new HashSet<>(List.of(genres)));
		}).filter((FilterFunction<Movie>)
				movie -> movie.getGenres().contains("Horror")
		).

//		dramaMovieSet.print();
		writeAsText("filter-output");
		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}
}
