package cs523.producer;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Producer {

	final Logger logger = LoggerFactory.getLogger(Producer.class);

	//client read from twitter
	private Client client;
	
	//create kafka producer
	private KafkaProducer<String, String> producer;
	
	private BlockingQueue<String> msgFromTwitter = new LinkedBlockingQueue<>(20);
	
	//set the keywords
	private List<String> trackTermsFromTwitter = Lists.newArrayList("covid", "vaccine", "pcr");

	
	//run of producer
	public static void main(String[] args) {
		new Producer().run();
	}

	// Twitter Client will send it to producer
	private Client createTwitterClient(BlockingQueue<String> msgQueue) {
		 
		
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hbEndpoint = new StatusesFilterEndpoint();
		
		// Term to get search from Twitter
		hbEndpoint.trackTerms(trackTermsFromTwitter);
		
		
		// Twitter tokens for Auth
		Authentication hosebirdAuth = new OAuth1("1R5FaVapm53Gnc0OgijvMmd28", 	"xlggPOkv9C61OcEW9JznROomkKPqPwZxBJAg25DsKwnUT7506y",
				"154526910-OBcHAEnnraYFLBj5YEZlw2KtoUULzROIBnkcorVG", 	"dLnUIaAE2t7jAlzTYNY3dkaSGBIMMtBi3kCbWcUNj6SEZ");

		
		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client").hosts(hosebirdHosts).authentication(hosebirdAuth)
				.endpoint(hbEndpoint).processor(new StringDelimitedProcessor(msgQueue));

		
		Client clt = builder.build();
		
		return clt;
	}

	
	// setting Kafka configs
	private KafkaProducer<String, String> createKafkaProducer() {

		Properties properties = new Properties();

		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAPSERVERS);
		
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		
		
		return new KafkaProducer<String, String>(properties);
	}


	private void run() {
		logger.info("Setting up");

		// Step 1 Call the Twitter Client
		client = createTwitterClient(msgFromTwitter);
		client.connect();

		// Step 2 Create Kafka Producer
		producer = createKafkaProducer();

		// on Shutdown 
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Application is not stopping!");
			client.stop();
			
			logger.info("Closing Producer");
			producer.close();
			
			logger.info("Finished closing");
		}));

		// Step 3 Send Tweets to Kafka
		while (!client.isDone()) 
			{
			
			String msg = null;
			
		 try {
			 msg = msgFromTwitter.poll(4,TimeUnit.SECONDS); 
			 }
			catch (InterruptedException e) 
		    {
			 e.printStackTrace();
			 client.stop();
			}
		 
		 
			if (msg != null)
			{
				logger.info(msg);
				JSONObject js = new JSONObject(msg);
				logger.info(js.toString());
				
				//serialize the Tweet 
				Tweet t = getTweet(js);
                if(t.getLang().equals("en")){
				logger.info(t.toString());

				producer.send(new ProducerRecord<String, String>( KafkaConfig.TOPIC, "", new Gson().toJson(t)), new Callback() {
						
						@Override
						public void onCompletion( RecordMetadata recordMetadata, Exception e) 
						{
								if (e != null)
								{ 
								logger.error( "Some error OR something bad happened", e);
								}
						 }
						});
				}
			}
		}
		logger.info("\n The Application Ended");
	}

	public static Tweet getTweet(JSONObject o) {

		Tweet getTweet = new Tweet();
		getTweet.setId(o.getString("id_str"));
		
		//print ID
		System.out.println("GET id: " + o.getString("id_str"));
		
		
		getTweet.setText(o.getString("text"));
		getTweet.setRetweet(getTweet.getText().startsWith("RT @"));
		getTweet.setInReplyToStatusId(o.get("in_reply_to_status_id").toString());

		JSONArray getHasTags = o.getJSONObject("entities").getJSONArray("hashtags");

		getHasTags.forEach( tag -> { getTweet.getHashTags().add(tag.toString()); 	});

		getTweet.setUsername(o.getJSONObject("user").getString("screen_name"));
		getTweet.setTimeStamp(o.getString("timestamp_ms"));
		getTweet.setLang(o.getString("lang"));

		return getTweet;
	}
}