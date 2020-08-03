import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import msg.kafka.com.Imessagebroker;
import msg.kafka.com.KafkaBroker;
import msg.kafka.com.PropertiesCache;

public class FileProducerTest {

	
	public static void main(String[] args) throws Exception
   {
		
		PropertiesCache properties1 = PropertiesCache.getInstance();
	
		
		  ClassLoader loader = Thread.currentThread().getContextClassLoader();
		  Properties properties2 = new Properties();
		  
		  try (InputStream resourceStream =loader.getResourceAsStream("config.properties")) 
		  {
		  properties2.load(resourceStream); 
		  } catch (IOException e)
		  {
		  e.printStackTrace(); 
		  }
		  
		  String server=properties2.getProperty("Broker-url");
		  
		  String inputfilepath=properties2.getProperty("inputfilepath");
		  String topicName=properties2.getProperty("topicName");
		  String key=properties2.getProperty("key");
		 
		
		String topic=properties1.getProperty("topicName");
		
		String filetopic=properties2.getProperty("fileTopic");
		System.out.println("kafka producer");
	  
		//creating broker object 
		Imessagebroker msgbroker=new KafkaBroker(server);
	 
		//connecting to broker
	  
		msgbroker.Connect();
	 
		boolean brokerconnection= msgbroker.isConnected();
	 
		System.out.println("Is broker connected:"+brokerconnection);
		File file=new File(inputfilepath);
		msgbroker.Publish(filetopic, key, file.getName());
		//try {
		// File file=new File(inputfilepath);
		 //if(file.isFile())
		 	//{   properties1.setProperty("outputname",file.getName());
		 		//msgbroker.Publish(filetopic, key, file.getName());
		 		//msgbroker.KafkaReadfileProducer(file, key, topic);	
			 
			// }
		//}
	// catch(Exception exc)
		//{
		 //System.out.println("unable to send file"+exc);
		//}
	 
	
	 
	 //closing connection
	 msgbroker.close();
		 
	
}
	
}
