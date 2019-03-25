package com.harsh.kafkatweeter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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
		consumerKey = props.getProperty("consumerAPIKey");
		consumerSecret = props.getProperty("consumerAPISecretKey");
		token = props.getProperty("accessToken");
		secret = props.getProperty("accessTokenSecret");
		
		props.forEach((key, value) -> System.out.println("Key: " + key + " and Value: " +value));
	}
	
	public void createTwitterClient() {
	}

	private void getProperties() {
		try (InputStream is = new FileInputStream("resources/config.properties");) {
			props.load(is);
		} catch (IOException io) {
			io.printStackTrace();
		}
	}
}
