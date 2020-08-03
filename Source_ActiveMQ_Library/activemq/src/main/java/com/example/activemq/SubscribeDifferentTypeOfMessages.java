package com.example.activemq;


import org.apache.activemq.command.ActiveMQBlobMessage;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQTextMessage;

import org.apache.maven.surefire.shade.org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public class SubscribeDifferentTypeOfMessages {
    private static final Logger LOGGER = LoggerFactory.getLogger(SubscribeDifferentTypeOfMessages.class);
    private FileAsByteArrayManager fileManager = new FileAsByteArrayManager();
    private PropertiesCache prop = PropertiesCache.getInstance();

    public SubscribeDifferentTypeOfMessages() {
    }

    void handleBytesMessage(ActiveMQBytesMessage bytesMessage, String filename) throws IOException, JMSException {
        String outputfileName = this.prop.getProperty("FileOutPutByteDirectory") + filename;
        this.fileManager.writeFile(bytesMessage.getContent().getData(), outputfileName);
        LOGGER.debug("Received ActiveMQBytesMessage message");
    }

    void handleBlobMessage(ActiveMQBlobMessage blobMessage, String filename) throws FileNotFoundException, IOException, JMSException {
        String outputfileName = this.prop.getProperty("FileOutPutByteDirectory") + filename;
        InputStream in = blobMessage.getInputStream();
        this.fileManager.writeFile(IOUtils.toByteArray(in), outputfileName);
        LOGGER.debug("Received ActiveMQBlobMessage message");
    }

    void handleObjectMessage(ActiveMQObjectMessage objectMessage) throws FileNotFoundException, IOException, JMSException {
        Object object = objectMessage.getObject();
        LOGGER.debug(String.format("Received ActiveMQObjectMessage [ %s ]", object));
    }

    void handleTextMessage(ActiveMQTextMessage txtMessage) throws JMSException {
        String msg = String.format("Received ActiveMQTextMessage [ %s ]", txtMessage.getText());
        this.prop.setProperty("FileName", txtMessage.getText());
        LOGGER.debug("Received message:" + msg);
    }

    public void handleBytesMessage1(ActiveMQBytesMessage bytesMessage, String filePath, String filename) throws IOException {
        String outputfileName = filePath + filename;
        this.fileManager.writeFile(bytesMessage.getContent().getData(), outputfileName);
        LOGGER.debug("Received ActiveMQBytesMessage message");
    }
}
