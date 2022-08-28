package com.rakeshv.flink.sources;

import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.io.InputStream;
import java.util.Properties;

public class TwitterSourceCreator {
    public static TwitterSource create() {
        Properties properties = new Properties();

        try {
//            InputStream is = TwitterSourceCreator
            properties.load(TwitterSourceCreator.class.getClassLoader().getResourceAsStream("application.properties"));
            properties.setProperty(TwitterSource.CONSUMER_KEY, properties.getProperty("api.key"));
            properties.setProperty(TwitterSource.CONSUMER_SECRET, properties.getProperty("api.secret.key"));
            properties.setProperty(TwitterSource.TOKEN, properties.getProperty("api.token"));
            properties.setProperty(TwitterSource.TOKEN_SECRET, properties.getProperty("api.secret.token"));
            return new TwitterSource(properties);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Couldn't load properties");
        }
    }
}
