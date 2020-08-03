package com.example.activemq;

import java.io.File;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.BlobMessage;
import org.apache.activemq.command.ActiveMQBlobMessage;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveMQBroker implements IMessageBroker, Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ActiveMQBroker.class);
    private String activeMqBrokerUri;
    private ActiveMQSession session;
    private MessageConsumer consumer;
    private MessageProducer msgProducer;
    private Connection connection;
    private MessageConsumer[] consumers;
    private String[] subscriptionNames;
    private Boolean isConnected = false;
    private Boolean isPublished = false;
    private String clientId;
    private Boolean isSessionCreated = false;
    private FileAsByteArrayManager fileManager = new FileAsByteArrayManager();
    private PropertiesCache prop = PropertiesCache.getInstance();
    private SubscribeDifferentTypeOfMessages subscribeDifferentTypeOfMessages = new SubscribeDifferentTypeOfMessages();
    Map<String, MessageProducer> producers = Collections.synchronizedMap(new HashMap());
    Thread shutdownHook = new Thread(new Runnable() {
        public void run() {
            try {
                ActiveMQBroker.this.closeConnection();
            } catch (JMSException var2) {
                var2.printStackTrace();
            }

        }
    });

    public ActiveMQBroker(String activeMqBrokerUri) {
        this.activeMqBrokerUri = activeMqBrokerUri;
    }

    public Boolean connect(String clientId) throws JMSException, URISyntaxException {
        this.clientId = clientId;

        try {
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(this.activeMqBrokerUri);
            this.connection = connectionFactory.createConnection();
            this.connection.setClientID(clientId);
            this.isConnected = true;
            this.session = (ActiveMQSession)this.connection.createSession(false, 1);
            this.connection.start();
            this.isSessionCreated = true;
            return true;
        } catch (Exception var3) {
            this.isConnected = false;
            this.isSessionCreated = false;
            return false;
        }
    }

    public void closeConnection() throws JMSException {
        try {
            if (this.msgProducer != null) {
                this.msgProducer.close();
            }

            if (this.session != null) {
                this.session.close();
            }

            if (this.connection != null) {
                this.connection.close();
                this.isConnected = false;
            }
        } catch (Throwable var2) {
            this.isConnected = true;
        }

    }

    public Boolean publish(String message, String queueName) throws JMSException {
        if (this.isConnected && this.isSessionCreated) {
            Instant start = Instant.now();
            this.msgProducer = (MessageProducer)this.producers.get(queueName);
            if (this.msgProducer == null) {
                Queue queue = this.session.createQueue(queueName);
                this.msgProducer = this.session.createProducer(queue);
                this.producers.put(queueName, this.msgProducer);
            }

            try {
                this.msgProducer.setDeliveryMode(1);
                TextMessage textMessage = this.session.createTextMessage(message);
                this.msgProducer.send(textMessage);
                Instant end = Instant.now();
                LOGGER.info("sendTextMessage for [" + message + "], took " + Duration.between(start, end));
                this.isPublished = true;
                return true;
            } catch (Exception var7) {
                this.isPublished = false;
                return false;
            }
        } else {
            this.isPublished = false;
            LOGGER.info("User must connected to the broker before publishing message");
            return false;
        }
    }

    public Boolean publishObjectMessage(Object object, String queueName) throws JMSException {
        if (this.isConnected && this.isSessionCreated) {
            Instant start = Instant.now();
            this.msgProducer = (MessageProducer)this.producers.get(queueName);
            if (this.msgProducer == null) {
                Queue queue = this.session.createQueue(queueName);
                this.msgProducer = this.session.createProducer(queue);
                this.producers.put(queueName, this.msgProducer);
            }

            try {
                this.msgProducer.setDeliveryMode(1);
                ObjectMessage Message = this.session.createObjectMessage((Serializable)object);
                this.msgProducer.send(Message);
                Instant end = Instant.now();
                LOGGER.info("sendObjectMessage for [" + object + "], took " + Duration.between(start, end));
                this.isPublished = true;
                return true;
            } catch (Exception var6) {
                this.isPublished = false;
                return false;
            }
        } else {
            this.isPublished = false;
            LOGGER.info("User must connected to the broker before publishing message");
            return false;
        }
    }

    public Boolean publishFileAsBlobMessage(File file, String queueName) throws JMSException {
        if (this.isConnected && this.isSessionCreated) {
            Instant start = Instant.now();
            this.msgProducer = (MessageProducer)this.producers.get(queueName);
            if (this.msgProducer == null) {
                Queue queue = this.session.createQueue(queueName);
                this.msgProducer = this.session.createProducer(queue);
                this.producers.put(queueName, this.msgProducer);
            }

            try {
                this.msgProducer.setDeliveryMode(1);
                BlobMessage blobMessage = this.session.createBlobMessage(file);
                this.prop.setProperty("FileName", file.getName());
                LOGGER.info("File name:" + this.prop.getProperty("FileName"));
                this.msgProducer.send(blobMessage);
                Instant end = Instant.now();
                LOGGER.info("sendFileAsBlobMessage for [" + file.getName() + "], took " + Duration.between(start, end));
                this.isPublished = true;
                return true;
            } catch (Exception var6) {
                this.isPublished = false;
                return false;
            }
        } else {
            this.isPublished = false;
            LOGGER.info("User must connected to the broker before publishing message");
            return false;
        }
    }

    public Boolean publishFileAsByteMessage(File file, String queueName) throws JMSException {
        if (this.isConnected && this.isSessionCreated) {
            Instant start = Instant.now();
            this.msgProducer = (MessageProducer)this.producers.get(queueName);
            if (this.msgProducer == null) {
                Queue queue = this.session.createQueue(queueName);
                this.msgProducer = this.session.createProducer(queue);
                this.producers.put(queueName, this.msgProducer);
            }

            try {
                this.msgProducer.setDeliveryMode(1);
                BytesMessage bytesMessage = this.session.createBytesMessage();
                this.prop.setProperty("FileName", file.getName());
                bytesMessage.writeBytes(this.fileManager.readfileAsBytes(file));
                this.msgProducer.send(bytesMessage);
                Instant end = Instant.now();
                LOGGER.info("sendFileAsBytesMessage for [" + file.getName() + "], took " + Duration.between(start, end));
                this.isPublished = true;
                return true;
            } catch (Exception var6) {
                this.isPublished = false;
                return false;
            }
        } else {
            this.isPublished = false;
            LOGGER.info("User must connected to the broker before publishing the message");
            return false;
        }
    }

    public void subscribe(String... queueNames) throws JMSException {
        if (this.isConnected && this.isSessionCreated) {
            this.consumers = new MessageConsumer[queueNames.length];

            try {
                for(int i = 0; i < queueNames.length; ++i) {
                    Queue queue = this.session.createQueue(queueNames[i]);
                    this.consumer = this.session.createConsumer(queue);
                    Message message = this.consumer.receive(1000L);
                    this.onMessage(message);
                }
            } catch (Exception var5) {
                LOGGER.debug("Error in subscription part:" + var5);
            }
        } else {
            LOGGER.info("User must connected to the broker before subscribing the message");
        }

    }

    public void subscribeFileFromBroker(String FilePath, String... queueNames) throws JMSException {
        if (this.isConnected && this.isSessionCreated) {
            this.consumers = new MessageConsumer[queueNames.length];

            try {
                for(int i = 0; i < queueNames.length; ++i) {
                    Queue queue = this.session.createQueue(queueNames[i]);
                    this.consumer = this.session.createConsumer(queue);
                    Message message = this.consumer.receive(1000L);
                    this.onFileMessage(message, FilePath);
                }
            } catch (Exception var6) {
                LOGGER.debug("Error in subscription part:" + var6);
            }
        } else {
            LOGGER.info("User must connected to the broker before subscribing the message");
        }

    }

    private void onFileMessage(Message message, String filePath) {
        try {
            String filename = this.prop.getProperty("FileName");
            Instant start = Instant.now();
            if (message instanceof BytesMessage) {
                this.subscribeDifferentTypeOfMessages.handleBytesMessage1((ActiveMQBytesMessage)message, filePath, filename);
            } else {
                System.out.println("test");
            }

            Instant end = Instant.now();
            System.out.println("Consumed message took " + Duration.between(start, end));
        } catch (Exception var6) {
            var6.printStackTrace();
        }

    }

    public synchronized void onException(JMSException ex) {
        System.out.println("JCG ActiveMQ JMS Exception occured.  Shutting down client.");
        LOGGER.debug("JCG ActiveMQ JMS Exception occured.  Shutting down client.");
    }

    public void onMessage(Message message) {
        try {
            String filename = this.prop.getProperty("FileName");
            Instant start = Instant.now();
            if (message instanceof TextMessage) {
                this.subscribeDifferentTypeOfMessages.handleTextMessage((ActiveMQTextMessage)message);
            } else if (message instanceof BlobMessage) {
                this.subscribeDifferentTypeOfMessages.handleBlobMessage((ActiveMQBlobMessage)message, filename);
            } else if (message instanceof BytesMessage) {
                this.subscribeDifferentTypeOfMessages.handleBytesMessage((ActiveMQBytesMessage)message, filename);
            } else if (message instanceof ObjectMessage) {
                this.subscribeDifferentTypeOfMessages.handleObjectMessage((ActiveMQObjectMessage)message);
            } else {
                System.out.println("test");
            }

            Instant end = Instant.now();
            System.out.println("Consumed message took " + Duration.between(start, end));
        } catch (Exception var5) {
            var5.printStackTrace();
        }

    }

    public Boolean isConnected() {
        return this.isConnected;
    }

    public void run() {
        while(true) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException var2) {
                var2.printStackTrace();
            }
        }
    }
}
