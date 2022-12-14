package cs523.tweets;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import kafka.serializer.StringDecoder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.*;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class Listener {

	public static void main(String[] args) throws Exception {
		
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Listener");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		JavaStreamingContext ssc = new JavaStreamingContext(jsc, Durations.seconds(5));


		Set<String> topics = new HashSet<>(Arrays.asList("covid".split(",")));
		
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  "StringDeserializer");
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "StringDeserializer");
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");

		
		//create stream from data coming from the producer
		JavaPairInputDStream<String, String> stream = KafkaUtils
				.createDirectStream(ssc, String.class, String.class,
						StringDecoder.class, StringDecoder.class, kafkaParams,
						topics);

		//stream create the RDD
		stream.foreachRDD(rdd -> {

			JavaRDD<Tweet> jrdd = rdd.map(f -> new Gson().fromJson(f._2, Tweet.class));
			
			jrdd.foreach(t -> {
			TweetHbaseTable.populateData(t);
			});
		});

		ssc.start();
		ssc.awaitTermination();
	}

	
	//for casting the tweet
	public static Tweet getTweet(JsonObject o) {
		Tweet tweet = new Tweet();

		tweet.setId(o.get("id").getAsString());

		
		tweet.setText(o.get("text").getAsString());
		tweet.setRetweet(tweet.getText().startsWith("RT @"));
		tweet.setInReplyToStatusId(o.get("in_reply_to_status_id").toString());

		JsonArray hasTags = o.get("entities").getAsJsonObject().get("hashtags").getAsJsonArray();
		hasTags.forEach(tag -> {tweet.getHashTags().add(hasTags.getAsJsonObject().get("text").getAsString());
		});

		
		
		tweet.setUsername(o.getAsJsonObject("user").get("screen_name").getAsString());
		tweet.setTimeStamp(o.get("timestamp_ms").getAsString());
		tweet.setLang(o.get("lang").getAsString());

		return tweet;
	}
}
