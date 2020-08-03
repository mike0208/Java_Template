package com.msgs.kafka;

import java.io.File;
import java.io.IOException;
import java.util.List;

public interface Imessagebroker {
    boolean Connect();

     boolean  Publish(String topic, String key, String Message);
     void  Subscribe(String Topic) throws InterruptedException;
     void close();
    void Subscribetomultipletopics(List<String> asList) throws InterruptedException ;
    void runConsumer() throws InterruptedException;
    public boolean isConnected();

    void FileProducer(
            File file, String key, String topicname);


    public void FileConsumer(String topic_name, String filepath) throws IOException;
}
