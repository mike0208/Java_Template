
import java.util.Arrays; 
import java.util.List; 
import java.util.Scanner;
import msg.kafka.com.Imessagebroker;
import msg.kafka.com.KafkaBroker;
  
  
  public class Consumer
  { 
	  public static void main(String[] args) throws InterruptedException {
  
  
  
 String TOPIC_NAME="kafkaneww";
  
  //String topic1="demo-new";
  //String topic2="demo-b";
  
  System.out.println("kafka consumer"); 
  
  Imessagebroker msgbroker =(Imessagebroker) new KafkaBroker("localhost:9092");
  
  //msgbroker.Subscribetomultipletopics(Arrays.asList(topic1,topic2));
  
  System.out.println("Subscribed to topic:"+TOPIC_NAME);
  
  //topic=sc.nextLine();
 msgbroker.Subscribe(TOPIC_NAME);
  
  msgbroker.close();
  
  
  
  } }
 