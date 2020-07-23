package com.parctice.kafka.streams;


import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;

import org.apache.kafka.common.serialization.*;

public class StreamsFilterTweets {

	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams");
		props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		
		KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
		
		KStream<String, String> filteredStream = inputTopic.filter(
				(k,jsonTweet) -> extractUserFollowers(jsonTweet) > 1000
				
		);
		
		filteredStream.to("important_tweets");
		
		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
		
		kafkaStreams.start();
		
		
	}
	
	private static JsonParser parser = new JsonParser();
	
	private static int extractUserFollowers(String tweetJson)	{
		
		try {
			return parser.parse(tweetJson)
			.getAsJsonObject()
			.get("user")
			.getAsJsonObject()
			.get("followers_count")
			.getAsInt();
		}
		catch(NullPointerException e)	{
			return 0;
		}
		
	}

}
