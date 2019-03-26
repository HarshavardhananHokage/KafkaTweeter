package com.harsh.kafkatweeter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {
	
	private Properties props = new Properties();
	
	private String consumerKey = "";
	private String consumerSecret = "";
	private String token = "";
	private String secret = "";
	
	public static void main(String[] args) {
		new TwitterProducer().run();
	}
	
	public void run() {
		getProperties();
		
		BlockingQueue<String> msgQueue = new LinkedBlockingDeque<String>(1000);
		Client client = createTwitterClient(msgQueue);
		KafkaProducer<String, String> twitterProducer = createKafkaProducer();
		
		client.connect();
		int count = 0;
		
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			client.stop();
			twitterProducer.close();
			System.out.println("All done!");
		}));
		
		while(!client.isDone() && count < 5) {
			try {
				String msg = msgQueue.poll(1L, TimeUnit.SECONDS);
				if(msg != null) {
					System.out.println(msg);
					count++;
					
					ProducerRecord<String, String> pr = new ProducerRecord<String, String>("twitter-topic", msg);
					
					twitterProducer.send(pr, new Callback() {
						
						@Override
						public void onCompletion(RecordMetadata metadata, Exception exception) {
							// TODO Auto-generated method stub
							if (exception != null) {
								exception.printStackTrace();
							}
						}
					});
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public Client createTwitterClient(BlockingQueue<String> msgQueue) {
		
		Hosts twitterHost = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint sfe = new StatusesFilterEndpoint();
		
		List<String> queryTerms = Arrays.asList("avengers");
		
		sfe.trackTerms(queryTerms);
		
		Authentication twitterAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
		
		ClientBuilder cb = new ClientBuilder().name("react-twitter-client")
				.hosts(twitterHost).authentication(twitterAuth).endpoint(sfe)
				.processor(new StringDelimitedProcessor(msgQueue));
		
		Client twitterClient = cb.build();
		
		return twitterClient;
	}

	private void getProperties() {
		try (InputStream is = new FileInputStream("resources/config.properties");) {
			props.load(is);
			consumerKey = props.getProperty("consumerAPIKey");
			consumerSecret = props.getProperty("consumerAPISecretKey");
			token = props.getProperty("accessToken");
			secret = props.getProperty("accessTokenSecret");
		} catch (IOException io) {
			io.printStackTrace();
		}
	}
	
	private KafkaProducer<String, String> createKafkaProducer() {
		
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Create Safe Producer
		props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		props.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		
		// Create Compressed Producer
		props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
		
		KafkaProducer<String, String> kfp = new KafkaProducer<String, String>(props);
		
		return kfp;
	}
}
