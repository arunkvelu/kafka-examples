package org.examples.producer;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerExample {
	KafkaProducer<String, String> producer = null;
	String topic = null;
	
	public static void main(String[] args) throws InterruptedException {
		KafkaProducerExample producer = new KafkaProducerExample("test");
		while(true) {
			producer.sendMessage("", String.valueOf(System.currentTimeMillis()));
			System.out.println("Message sent");
			Thread.sleep(1000);
		}
	}
		
	public KafkaProducerExample(String topic) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
	
		this.producer = new KafkaProducer<>(props);
		this.topic = topic;
	}
	
	public boolean sendMessage(String key, String value) {
		producer.send(new ProducerRecord<>(topic, key, value));
		producer.flush();
		return true;
	}
}
