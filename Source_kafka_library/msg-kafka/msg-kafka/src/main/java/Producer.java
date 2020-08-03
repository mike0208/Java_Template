
  import java.util.Properties; 
  import java.util.Scanner;
  import java.util.concurrent.ExecutionException;
  
  import org.apache.kafka.clients.admin.AdminClient; 
  import org.apache.kafka.clients.admin.ListTopicsOptions;
  
  import msg.kafka.com.Imessagebroker;
  import msg.kafka.com.KafkaBroker;
  
  public class Producer 
  { 
	  public static void main(String[] args) throws InterruptedException
  	{
  
  
	  String key="b"; 
	  String TOPIC_NAME="kafkaneww";
	  String MESSAGE="hello kafka demo !";
  
	  System.out.println("kafka producer");
  
	  Imessagebroker msgbroker=new KafkaBroker("localhost:9092");
	  msgbroker.Connect();
	  boolean brokerconnection= msgbroker.isConnected();
	  System.out.println("Is broker connected:"+brokerconnection);
  
  
	  msgbroker.Publish(TOPIC_NAME,key,MESSAGE);
 // System.out.println(ispublished); 
  
	  msgbroker.close();
  
  
  
		/*
		 * Scanner sc=new Scanner(System.in); System.out.println("kafka producer");
		 * Imessagebroker messageBroker=new KafkaBroker("localhost:9092");
		 * messageBroker.Connect();
		 */
  
  
		/*
		 * System.out.println("enter the topic to publish"); String topic=
		 * sc.nextLine(); System.out.println("enter the key to publish"); String key=
		 * sc.nextLine();
		 * System.out.printf("enter message to publish on topic:%1$s"+"\r\n",topic);
		 * String message= sc.nextLine(); boolean ispublished=
		 * messageBroker.Publish(topic,key,message); System.out.println(ispublished);
		 * messageBroker.close();
		 */
  
  } }
 