package org.examples.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class KafkaConsumerWithListeners<K, V> {

	KafkaConsumer<K, V> consumer = null;
	String topic = null;

	public static void main(String[] args) {
		KafkaConsumerExample<String, String> consumer = new KafkaConsumerExample<>("test");
		while (true) {
			consumer.consumeDisplayMessages();
		}
	}

	public KafkaConsumerWithListeners(String topic) {
		this.topic = topic;

		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
		properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-example-consumer");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Arrays.asList(this.topic), new ConsumerRebalanceListenerImpl());
	}

	public void consumeDisplayMessages() {
		ConsumerRecords<K, V> recordsList = consumer.poll(Duration.ofSeconds(3));
		for (ConsumerRecord<K, V> record : recordsList) {
			System.out.println("Topic: " + record.topic() + ", Partition: " + record.partition() + ", Offset: "
					+ record.offset() + ", Value: " + record.value());
		}
	}

	private static class ConsumerRebalanceListenerImpl implements ConsumerRebalanceListener {
		/**
		 * A callback method implementation to provide handling of
		 * offset commits to a customized store on the start of a rebalance
		 * operation. This method will be called before a rebalance operation
		 * starts and after the consumer stops fetching data. It is recommended
		 * that offsets should be committed in this callback to either Kafka or
		 * a custom offset store to prevent duplicate data.
		 **/
		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			System.out.println(String.format("Partitions revoke listener: %s", partitions.toString()));
			// Add your operations here if any
		}

		/**
	     * A callback method implementation to provide handling of customized offsets on completion of a successful
	     * partition re-assignment. This method will be called after the partition re-assignment completes and before the
	     * consumer starts fetching data, and only as the result of a poll(long) call.
	     **/
		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			System.out.println(String.format("Partitions assignment listener: %s", partitions.toString()));
			// Add your operations here if any
		}
	}
}
