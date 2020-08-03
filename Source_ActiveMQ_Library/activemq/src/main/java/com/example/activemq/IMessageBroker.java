package com.example.activemq;

import javax.jms.JMSException;
import java.io.File;
import java.net.URISyntaxException;

public interface IMessageBroker {
    Boolean connect(String clientId) throws JMSException, URISyntaxException;

    Boolean publish(String message, String queueName) throws JMSException;

    Boolean publishObjectMessage(Object object, String queueName) throws JMSException;

    Boolean publishFileAsBlobMessage(File file, String queueName) throws JMSException;

    Boolean publishFileAsByteMessage(File file, String queueName) throws JMSException;

    void closeConnection() throws JMSException;

    void subscribe(String... queues) throws JMSException;

    void subscribeFileFromBroker(String FilePath, String... queues) throws JMSException;

    Boolean isConnected();
}
