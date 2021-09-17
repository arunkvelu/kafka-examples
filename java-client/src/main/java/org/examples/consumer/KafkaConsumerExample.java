package org.examples.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerExample<K, V> {
	KafkaConsumer<K, V> consumer = null;
	String topic = null;
	
	public static void main(String[] args) {
		KafkaConsumerExample<String, String> consumer = new KafkaConsumerExample<>("test");
		while(true) {
			consumer.consumeDisplayMessages();
		}
		
	}
	
	public KafkaConsumerExample(String topic) {
		this.topic = topic;
		
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
		properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-example-consumer");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Arrays.asList(this.topic));
	}
	
	public void consumeDisplayMessages() {
		ConsumerRecords<K, V> recordsList = consumer.poll(Duration.ofSeconds(3));
		for(ConsumerRecord<K, V> record: recordsList) {
			System.out.println("Topic: "+record.topic()+", Partition: "+record.partition()+", Offset: "+record.offset()+", Value: "+record.value());
		}
	}

}
