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

package projekat;

import models.Bus;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.Objects;
import java.util.Properties;

import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import static java.lang.System.exit;

public class DataStreamJob {
	public static void main(String[] args) throws Exception {
		Double lat1 = null, lat2 = null, long1 = null, long2 = null;
		for(String a : args){
			long1 = Double.parseDouble(args[0]);
			long2 = Double.parseDouble(args[1]);
			lat1 = Double.parseDouble(args[2]);
			lat2 = Double.parseDouble(args[3]);
		}
		if(lat1 == null || lat2 == null || long1 == null || long2 == null )
		{
			exit(1);
		}
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		String inputTopic = "locations";
		String server = "kafka:9092";

		DataStream<String> dataStream = StreamConsumer(inputTopic, server, env);
		DataStream<Bus> busStream = ConvertStreamFromJsonToBusType(dataStream);

		Double finalLat1 = lat1;
		Double finalLat2 = lat2;
		Double finalLong1 = long1;
		Double finalLong2 = long2;
		SingleOutputStreamOperator windowedStream = busStream
				.filter(new FilterFunction<Bus>() {
					@Override
					public boolean filter(Bus value) throws Exception {
						Double currLat = value.getLatitude();
						Double currLong = value.getLongitude();
						return currLong < finalLong1 && currLong > finalLong2 && currLat < finalLat1 && currLat > finalLat2;
					}
				})
				.keyBy(Bus::getBusLine)
				.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(10)))
				.aggregate(new AverageAggregate());

		SingleOutputStreamOperator windowedStream2 = busStream
				.keyBy(Bus::getBusLine)
				.window(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(30)))
				.aggregate(new TopNLocationsAggregate(3));

		windowedStream2.print();
		CassandraService cassandraService = new CassandraService();
		cassandraService.sinkToCassandraDB(windowedStream);
		cassandraService.sinkToCassandraDB2(windowedStream2);

		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}
	public static DataStream<String> StreamConsumer(String inputTopic, String server, StreamExecutionEnvironment environment) throws Exception {
		FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(inputTopic, server);
		DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);


		return stringInputStream.map(new MapFunction<String, String>() {
			private static final long serialVersionUID = -999736771747691234L;

			@Override
			public String map(String value) throws Exception {
				return value;
			}
		});
	}
	public static FlinkKafkaConsumer<String> createStringConsumerForTopic(String topic, String kafkaAddress) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", kafkaAddress);
		FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
				topic, new SimpleStringSchema(), props);

		return consumer;
	}
	public static DataStream<Bus> ConvertStreamFromJsonToBusType(DataStream<String> jsonStream) {
		return jsonStream.map(kafkaMessage -> {
			try {
				JsonNode jsonNode = new ObjectMapper().readValue(kafkaMessage, JsonNode.class);
				SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				return Bus.builder()
						.Latitude(jsonNode.get("latitude").asDouble())
						.Longitude(jsonNode.get("longitude").asDouble())
						.Speed(jsonNode.get("speed").asDouble())
						.BusID(jsonNode.get("busID").asText())
						.BusLine(jsonNode.get("busLine").asText())
						.Date(jsonNode.get("date").asText())
						.Time(jsonNode.get("time").asText())
						.Timestamp(jsonNode.get("ts").asInt())
						.build();

			} catch (Exception e) {
				return null;
			}
		}).filter(Objects::nonNull).forward();
	}
}
