package com.harsh.kafkaintro.kafka_intro;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerTest {

	public static void main(String[] args) {
		
		final Logger logger = LoggerFactory.getLogger(KafkaConsumerTest.class.getName());
		
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "javaGroup");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
		consumer.subscribe(Collections.singleton("first_topic"));
		
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
			
			for(ConsumerRecord<String, String> record: records) {
				logger.info("Key: " + record.key() + ", Value: " + record.value());
			}
		}
	}
}
