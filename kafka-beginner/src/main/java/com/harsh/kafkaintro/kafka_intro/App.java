package com.harsh.kafkaintro.kafka_intro;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        Properties kafkaProducerProperties = new Properties();
        
        kafkaProducerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kafkaProducerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        KafkaProducer<String, String> simpleProducer = new KafkaProducer<String, String>(kafkaProducerProperties);
        
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello Java");
        
        simpleProducer.send(record);
        
        simpleProducer.flush();
        simpleProducer.close();
    }
}
