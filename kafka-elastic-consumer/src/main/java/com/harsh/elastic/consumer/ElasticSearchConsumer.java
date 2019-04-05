package com.harsh.elastic.consumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import com.google.gson.JsonParser;

public class ElasticSearchConsumer {
	
	private String hostName = "";
	private String userName = "";
	private String password = "";
	
	public static void main(String[] args) {
		
		new ElasticSearchConsumer().run();
		
	}
	
	public void run() {
		loadProps();
		writeToElasticSearch();
	}
	
	private void writeToElasticSearch() {
		String test = "";
		var kafkaConsumer = getKafkaConsumer();
		kafkaConsumer.subscribe(Arrays.asList("twitter-topic"));
		
		RestHighLevelClient client = getElasticRestClient();
		
		while(true) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
			
			for(ConsumerRecord<String, String> record : records) {
				try {
					String tweetID = getIdFromTweet(record.value());
					IndexRequest req = new IndexRequest("twitter", "tweet", tweetID).source(record.value(), XContentType.JSON);
					IndexResponse res = client.index(req, RequestOptions.DEFAULT);
					System.out.println(res.getId());
				} catch (IOException io) {
					io.printStackTrace();
				}
			}	
		}
	}
	
	private JsonParser jsonParser = new JsonParser();
	private String getIdFromTweet(String tweetJSON) {
		
		return jsonParser.parse(tweetJSON).getAsJsonObject().get("id_str").getAsString();
	}
	
	private KafkaConsumer<String, String> getKafkaConsumer() {
		
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "twitter-group");
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		var consumer = new KafkaConsumer<String, String>(props);
		return consumer;
	}
	
	private RestHighLevelClient getElasticRestClient() {
		
		final CredentialsProvider credProv = new BasicCredentialsProvider();
		credProv.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
		
		RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(hostName, 443, "https"))
				.setHttpClientConfigCallback(new HttpClientConfigCallback() {
					
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						// TODO Auto-generated method stub
						return httpClientBuilder.setDefaultCredentialsProvider(credProv);
					}
				});
		
		RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);
		return client;
	}
	
	private void loadProps() {
		
		try (InputStream is = new FileInputStream("resources/config.properties");) {
			Properties props = new Properties();
			props.load(is);
			
			hostName = props.getProperty("elasticHostName");
			userName = props.getProperty("elasticUserName");
			password = props.getProperty("elasticPassword");
			
			// System.out.printf("%s\t%s\t%s", hostName, userName, password);
			
		} catch(IOException io) {
			io.printStackTrace();
		}
	}
	
	

}
