package msg.kafka.com;

import java.io.File;
import java.io.IOException;
import java.util.List;

public interface Imessagebroker
{
    public boolean Connect();
   
    public boolean Publish(String topic, String key, String Message);
    
    public void  Subscribe(String Topic) throws InterruptedException;
    
    public void close();

    public void runConsumer() throws InterruptedException;
  
    public boolean isConnected();
	
	void KafkaReadfileProducer(File file,String key,String topicname);
	   
	public void FileConsumer(String topic_name, String filepath) throws IOException;
	
	void Subscribetomultipletopics(List<String> asList) throws InterruptedException ;
	
	
    
}
