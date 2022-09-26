package cs523.producer;

public class KafkaConfig {

	//kafka port 
    public static final String BOOTSTRAPSERVERS  = "127.0.0.1:9092";
    //topic of the project
    public static final String TOPIC = "covid";
    
    //Deserialization 
    public static final String BOOTSTRAP_SERVERS_CONFIG = "StringDeserializer";
    public static final String  VALUE_DESERIALIZER_CLASS_CONFIG = "StringDeserializer";

}
