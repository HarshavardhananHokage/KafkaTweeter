package com.harsh.kafkaintro.kafka_intro;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class ProducerWithKeys 
{
    public static void main( String[] args )
    {
    	final Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class.getName());
    	// logger
    	
        Properties kafkaProducerProperties = new Properties();
        
        kafkaProducerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kafkaProducerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        KafkaProducer<String, String> simpleProducer = new KafkaProducer<String, String>(kafkaProducerProperties);
       
        for(int i = 0; i < 10; i++) {
        	ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "id_" + i, "Hello World " + i);
            
            simpleProducer.send(record, new Callback() {
    			
    			public void onCompletion(RecordMetadata metadata, Exception exception) {
    				// TODO Auto-generated method stub
    				if(exception == null) {
    					logger.info("Message produced successfully");
    					logger.info("To Topic: " + metadata.topic());
    					logger.info("To Partition: " + metadata.partition());
    					logger.info("With Offset: " + metadata.offset());
    					logger.info("At Timestamp: " + metadata.timestamp());	
    					System.out.println("asddasddsadsad");
    				} else {
    					logger.error("Error on message production: ", exception);
    				}
    			}
    		});
	
        }        
        simpleProducer.flush();
        simpleProducer.close();
    }
}
