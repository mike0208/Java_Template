import java.io.IOException;

import java.io.InputStream;

import java.util.Properties;

import msg.kafka.com.Imessagebroker;
import msg.kafka.com.KafkaBroker;
import msg.kafka.com.PropertiesCache;

public class FileConsumerTest {

	 public static void main(String[] args) throws InterruptedException, IOException {
		  

		PropertiesCache properties2 = PropertiesCache.getInstance();
		
			
		 String server=properties2.getProperty("Broker-url");
		 String topic=properties2.getProperty("topicName");
		 String outputfilepath= properties2.getProperty("outputfilepath");
		 String filetopic=properties2.getProperty("fileTopic");
		 
		 
		 
		  System.out.println("kafka consumer"); 
		  
		  
		  Imessagebroker msgbroker = (Imessagebroker) new KafkaBroker(server);
		  
		  System.out.println("Subscribed to topic:"+topic);
		  
		  msgbroker.Subscribe(filetopic);
		  
		  msgbroker.FileConsumer(topic,outputfilepath);
		  
		  msgbroker.close();
		  
		  
		  
		  
		  
	 }
}
