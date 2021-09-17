package org.examples.streams;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

public class KafkaStreamsExample {
	public static void main(String[] args) {
		KafkaStreamsExample.startStreaming();
	}
	
	public static void startStreaming() {
		Properties properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-example-app");
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
		properties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
		properties.put(StreamsConfig.CLIENT_ID_CONFIG, "streams-example");
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		/**
		 * Source topic: test - Contains milliseconds as messages in String format
		 * Target topic: result-test - Contains date string as messages
		 * Conversion: Fetch milliseconds from source topic, convert to date representation and store it in target topic 
		 */
		streamsBuilder.stream("test")
						.mapValues(value -> new Date(Long.parseLong((String) value)).toString())
						.to("result-test");
		
		Topology topology = streamsBuilder.build();
		
		KafkaStreams streams = new KafkaStreams(topology, properties);
		streams.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread("close-streams") {
			@Override
			public void run() {
				streams.close();
				System.out.print("Stream closed!");
			}
		});
		
		/**
		 * //Java 8
		 * Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
		*/
	}
}
