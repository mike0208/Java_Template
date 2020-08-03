package msg.kafka.com;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaBroker implements Imessagebroker {
	
	// private String DEFAULT_BOOTSTRAP_SERVER="localhost:9092";
	
	private String DEFAULT_BOOTSTRAP_SERVER;
	private KafkaProducer<String, String> producer;
	private KafkaProducer<String, String> byteproducer;

	private String BootstrapServer;
	private final Logger mLogger = LoggerFactory.getLogger(KafkaBroker.class);
	private KafkaConsumer<String, String> consumer;
	private String groupid = "java-kafka";
	private Boolean isConnected = false;
	final int ADMIN_CLIENT_TIMEOUT_MS = 20000;
	private boolean isPublished = false;
	private PropertiesCache propapp = PropertiesCache.getInstance();
	private String filePath;
	private BufferedWriter buffWriter;

	private List<String> topics;
	

	public KafkaBroker(String BootstrapServer) {
		
		if (BootstrapServer != null && !BootstrapServer.isEmpty()) {
			DEFAULT_BOOTSTRAP_SERVER = BootstrapServer;
		}
	}

	@Override
	// Creating a kafka producer
	public boolean Connect()

	{
		Properties props = producerProps(DEFAULT_BOOTSTRAP_SERVER);
		try (AdminClient client = AdminClient.create(props)) {
			producer = new KafkaProducer<>(props);
			mLogger.info("Producer initialized");
			client.listTopics(new ListTopicsOptions().timeoutMs(ADMIN_CLIENT_TIMEOUT_MS)).listings().get();
			isConnected = true;
			return isConnected;
		}

		catch (Exception exc) {
			System.out.printf("Kafka is not available, timed out after {} ms", ADMIN_CLIENT_TIMEOUT_MS);
			isConnected = false;
			return isConnected;
		}

	}

	
	// defining producer config properties
	
	private Properties producerProps(String default_bootstrap_server) 
	{
		String serializer = StringSerializer.class.getName();
		Properties props = new Properties();

		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVER);
		props.put("connections.max.idle.ms", 10000);
		props.put("request.timeout.ms", 5000);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);
		return props;
	}
	

	public boolean Publish(String topic, String key, String message) {
	try {
			long startTime = System.currentTimeMillis();
			mLogger.info("publish message:" + message + "for key:" + key + "\non topic:" + topic);
			ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
			//Future<RecordMetadata> future =
			producer.send(record, new Democallback(startTime, key, message));
			
			
			isPublished = true;
		return isPublished;
			
	} catch (Exception exc)
	{
			System.out.printf("msg is not published");
			isPublished = false;
		return isPublished;
		}
	}


	public class Democallback implements Callback {
		private final long startTime;
		private final String key;
		private final String message;

		public Democallback(long startTime, String key, String message) {
			this.startTime = startTime;
			this.key = key;
			this.message = message;
		}

		public void onCompletion(RecordMetadata metadata, Exception exception) {
			long elapsedTime = System.currentTimeMillis() - startTime;
			mLogger.info("Called Callback method mmk");

			if (metadata != null) {
				System.out.println("message(" + key + ", " + message + ") sent to partition(" + metadata.partition()
						+ "), " + "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
			} else {
				exception.printStackTrace();
			}
		}
	}

	
	@Override
	public void Subscribe(String Topic) throws InterruptedException {

		if (consumer == null) {

			Properties props = consumerProps(DEFAULT_BOOTSTRAP_SERVER, groupid);
			consumer = new KafkaConsumer<String, String>(props);

		}
		consumer.subscribe(Collections.singleton(Topic));
		runConsumer();

	}
	private Properties consumerProps(String default_bootstrap_server, String groupid) 
	{
		String deserializer = StringDeserializer.class.getName();
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVER);
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupid);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return properties;
			
	}
	
	public void runConsumer() throws InterruptedException {

		final int max = 5;
		int noRecordsCount = 0;

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
			// 100 is the time in milliseconds consumer will wait if no record is found at
			// broker.

			// mLogger.info("Found {} records in kafka", records.count());
			if (records.count() == 0) {
				noRecordsCount++;
				if (noRecordsCount > max)
					break;
				// If no message found count is reached to threshold exit loop.
				else
					continue;
			}
			// print each record
			records.forEach(record -> {
				System.out.printf("Consumer Record:(%s, %s, %d, %d)\n", record.key(), record.value(),
						record.partition(), record.offset());
				//set property
				propapp.setProperty("FileName", record.value());
			});

		}
		try {
			// commits the offset of record to broker.
			consumer.commitSync();
			System.out.println("finished sync");

		} catch (CommitFailedException e) {
			mLogger.error("commit failed", e);
		}

		//consumer.close();
		//System.out.println("DONE");
	}
	
	
	public void KafkaReadfileProducer( File file,String key, String topic) {
		
				
       propapp.setProperty("FileName", file.getName());
              
       String filename=propapp.getProperty("FileName");
             
       long startTime = System.currentTimeMillis();
		System.out.println("publish message on file :"+filename+"\ton topic:"+topic);
	 

		 // Read each line from the file and send via the producer
		
        try(BufferedReader br = new BufferedReader(new FileReader(file)))
        
        {
          String line = br.readLine();
          
         while (line != null) 
            {
        	 ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, key, line);
        	 
        	 
		     producer.send(data,new FileDemocallback(startTime, key, line));
		     
		     
		     System.out.println(line);
		     
		     line = br.readLine();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
}
	
	public class FileDemocallback implements Callback {
		private final long startTime;
		private final String key;
		private  String message;

		public FileDemocallback(long startTime, String key, String message ) {
			this.startTime = startTime;
			this.key = key;
			this.message = message;
		}

		public void onCompletion(RecordMetadata metadata, Exception exception) {
			long elapsedTime = System.currentTimeMillis() - startTime;
			System.out.println("Called Callback method");

			if (metadata != null) {
				System.out.println("message(" + key + ", " + message + ") sent to partition(" + metadata.partition()
						+ "), " + "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
			} else {
				exception.printStackTrace();
			}
		}

	}
	
		
	 public void FileConsumer(String topic, String filePath) throws IOException {

		 //buffWriter = new BufferedWriter(new FileWriter(filePath));
		 
		 
		
		 if (consumer == null) {

				Properties props = consumerProps(DEFAULT_BOOTSTRAP_SERVER, groupid);
				consumer = new KafkaConsumer<String, String>(props);

			}
				 
		 consumer.subscribe(Collections.singleton(topic));
		 runFileConsumer(filePath);
			
			       
	 }
	private void runFileConsumer(String filePath) throws IOException {
		
		String outputfilename=filePath+propapp.getProperty("FileName");
		buffWriter = new BufferedWriter(new FileWriter(outputfilename));
		
		final int max = 5;
		int noRecordsCount = 0;

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

			if (records.count() == 0) {
				noRecordsCount++;
				if (noRecordsCount > max)
					break;
				// If no message found count is reached to threshold exit loop.
				else
					continue;
			}

			// print each record
						records.forEach(record -> {
							System.out.printf("Consumer Record:(%s, %s, %d, %d)\n", record.key(), record.value(),
									record.partition(), record.offset());
							try {
								buffWriter.write(record.value() + System.lineSeparator());
								 buffWriter.flush();
								
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
				            
						});
						
						

		}

		try {

			consumer.commitSync();
			System.out.println("finished sync");
		}

		catch (CommitFailedException e) 
		{
			mLogger.error("commit failed", e);
		} 
		
					
	}

	

	public void run() throws InterruptedException {

	}

		public void close() {
		
		try{
			
			if(producer!=null)
		{
				
				producer.close();
		}
		if(consumer!=null)
		{
			System.out.println("Closing consumer's connection");
		    consumer.close();
		    System.out.println("DONE");
		}
		}
		catch(Exception ex)
		{
			mLogger.info("cannot close the connection");
						
		}
				
	}
	
	@Override
	public void Subscribetomultipletopics(List<String> asList) throws InterruptedException {

		int topicslength = asList.size();

		for (int i = 0; i < topicslength; ++i) {
			if (consumer == null) {

				Properties props = consumerProps(DEFAULT_BOOTSTRAP_SERVER, groupid);
				consumer = new KafkaConsumer<String, String>(props);
			}
			consumer.subscribe(asList);
			runmultipletopicsConsumer();
		}
	}

	private void runmultipletopicsConsumer() {
		final int max = 5;
		int noRecordsCount = 0;

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

			if (records.count() == 0) {
				noRecordsCount++;
				if (noRecordsCount > max)
					break;
				// If no message found count is reached to threshold exit loop.
				else
					continue;
			}

			for (TopicPartition partition : records.partitions()) {
				List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
				for (ConsumerRecord<String, String> record : partitionRecords) {
					System.out.println(record.offset() + ": " + record.value());
				}
				long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();

				//System.out.println(lastOffset);
				consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
			}

		}

		try {

			consumer.commitSync();
			System.out.println("finished sync");
		}

		catch (CommitFailedException e) {
			mLogger.error("commit failed", e);
		}
	}

	@Override
	public boolean isConnected() {
		// TODO Auto-generated method stub
		return isConnected;
	}

	

	/*
	 * public void produceMessages(String filename,String key,String topicname) {
	 * long startTime=System.currentTimeMillis();
	 * 
	 * String serializer = StringSerializer.class.getName(); // Setup the Kafka
	 * Properties props2 = new Properties();
	 * props2.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,DEFAULT_BOOTSTRAP_SERVER);
	 * props2.put("serializer.class","kafka.serializer.StringEncoder");
	 * props2.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
	 * props2.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);
	 * // ProducerConfig config = new ProducerConfig(props);
	 * 
	 * producer=new KafkaProducer<>(props2);
	 * 
	 * // Read each line from the file and send via the producer try(BufferedReader
	 * br = new BufferedReader(new FileReader(filename) )) { String line =
	 * br.readLine(); while (line != null) {
	 * 
	 * 
	 * ProducerRecord<String, String> data = new ProducerRecord<String,
	 * String>(topicname, key, line); producer.send(data,new
	 * Democallback(startTime,key,line)); System.out.println(line);
	 * //Thread.sleep(200l); line = br.readLine(); } } catch (IOException e) {
	 * e.printStackTrace(); }
	 * 
	 * }
	 */
	
	/*
	 * public void produceMessages() {
	 * 
	 * // Setup the Kafka Producer Properties props = new Properties();
	 * props.put("metadata.broker.list", getKafkaHostname() + ":" + getKafkaPort());
	 * props.put("serializer.class", "kafka.serializer.StringEncoder");
	 * ProducerConfig config = new ProducerConfig(props); Producer<String, String>
	 * producer = new Producer<String, String>(config);
	 * 
	 * // Read each line from the file and send via the producer try(BufferedReader
	 * br = new BufferedReader(new FileReader(getInputFileName()))) { String line =
	 * br.readLine(); while (line != null) { KeyedMessage<String, String> data = new
	 * KeyedMessage<String, String>(getTopic(), null, line); producer.send(data);
	 * System.out.println(line); //Thread.sleep(200l); line = br.readLine(); } }
	 * catch (IOException e) { e.printStackTrace(); } //catch (InterruptedException
	 * e) {}
	 * 
	 * }
	 */
	
	

	
	
		
	/*
	 * long startTime = System.currentTimeMillis();
	 * 
	 * String serializer = StringSerializer.class.getName(); // Setup the Kafka
	 * Properties props2 = new Properties();
	 * props2.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
	 * DEFAULT_BOOTSTRAP_SERVER); props2.put("batch.size", 26214400);
	 * props2.put("linger.ms", 1); props2.put("buffer.memory", 2 * 26214400);
	 * props2.put("max.request.size", 26214400);
	 * props2.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
	 * props2.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
	 * "org.apache.kafka.common.serialization.ByteArraySerializer");
	 * KafkaProducer<String, byte[]> producer = new KafkaProducer<String,
	 * byte[]>(props2);
	 * 
	 * // Read each line from the file and send via the producer
	 * 
	 * try { byte[] temp = Files.readAllBytes(Paths.get(filename));
	 * 
	 * // producer.send(new ProducerRecord<String,byte[]>("audio-queue", //
	 * "test-key",temp )); publishMessage(filename, topicname, temp); }
	 * 
	 * try(BufferedReader br = new BufferedReader(new FileReader(filename) )) {
	 * String line = br.readLine(); while (line != null) {
	 * 
	 * 
	 * ProducerRecord<String, String> data = new
	 * ProducerRecord<String,String>(topicname, key, line);
	 * 
	 * System.out.println(line); //Thread.sleep(200l); line = br.readLine(); } }
	 * 
	 * catch (IOException e) { e.printStackTrace(); }
	 * 
	 * }
	 * 
	 */
	  
	 

	

	/*
	 * @Override public boolean publishMessage(String filename, String topic, byte[]
	 * temp) { try {
	 * 
	 * long startTime=System.currentTimeMillis(); mLogger.info("publish message:"
	 * +filename+"on topic:"+topic);
	 * 
	 * ProducerRecord<String, byte[]> data = new ProducerRecord<String,
	 * byte[]>(topic, filename, temp);
	 * 
	 * byteproducer.send(data,new Democallback2(startTime,filename,data));
	 * 
	 * return isPublished=true; } catch(Exception exc) {
	 * System.out.printf("msg is not published"); isPublished=false; return
	 * isPublished; }
	 * 
	 * }
	 */

	
	
	/*
	 * public boolean Connect2()
	 * 
	 * { Properties props = producerProps2(DEFAULT_BOOTSTRAP_SERVER);
	 * 
	 * try (AdminClient client = AdminClient.create(props)) { KafkaProducer<String,
	 * byte[]> byteproducer = new KafkaProducer<String, byte[]>(props);
	 * mLogger.info("Producer initialized"); client.listTopics(new
	 * ListTopicsOptions().timeoutMs(ADMIN_CLIENT_TIMEOUT_MS)).listings().get();
	 * isConnected = true; return true; }
	 * 
	 * catch (Exception exc) {
	 * System.out.printf("Kafka is not available, timed out after {} ms",
	 * ADMIN_CLIENT_TIMEOUT_MS); isConnected = false; return isConnected; }
	 * 
	 * // return isConnected;
	 * 
	 * }
	 */
	
	/*
	 * public boolean Connect2()
	 * 
	 * { Properties props = producerProps2(DEFAULT_BOOTSTRAP_SERVER);
	 * 
	 * try (AdminClient client = AdminClient.create(props)) { byteproducer = new
	 * KafkaProducer<String, String>(props); mLogger.info("Producer initialized");
	 * client.listTopics(new
	 * ListTopicsOptions().timeoutMs(ADMIN_CLIENT_TIMEOUT_MS)).listings().get();
	 * isConnected = true; return true; }
	 * 
	 * catch (Exception exc) {
	 * System.out.printf("Kafka is not available, timed out after {} ms",
	 * ADMIN_CLIENT_TIMEOUT_MS); isConnected = false; return isConnected; }
	 * 
	 * // return isConnected;
	 * 
	 * }
	 */
	
	
	
	
	/* org.apache.kafka.common.serialization.ByteArraySerializer */
	
	

	/*
	 * private Properties producerProps2(String default_bootstrap_server) {
	 * Properties props2 = new Properties();
	 * 
	 * String serializer = StringSerializer.class.getName();
	 * props2.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
	 * DEFAULT_BOOTSTRAP_SERVER); props2.put("acks", "all"); props2.put("retries",
	 * 0); props2.put("batch.size", 26214400); props2.put("linger.ms", 1);
	 * props2.put("buffer.memory", 2 * 26214400); props2.put("max.request.size",
	 * 26214400); props2.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
	 * serializer); props2.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
	 * serializer); return props2;
	 * 
	 * }
	 */
	
	
	

}
