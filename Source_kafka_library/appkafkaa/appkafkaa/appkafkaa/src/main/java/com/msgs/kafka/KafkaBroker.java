package com.msgs.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;


public class KafkaBroker implements Imessagebroker {
    private String DEFAULT_BOOTSTRAP_SERVER;
    private KafkaProducer<String, String> producer;
    private final Logger mLogger = LoggerFactory.getLogger(Producer.class);
    private KafkaConsumer<String, String> consumer;
    private String groupid = "java-kafka";
    final int ADMIN_CLIENT_TIMEOUT_MS = 20000;
    private Boolean isConnected = false;
    private boolean isPublished = false;
    private PropertiesCache propapp = PropertiesCache.getInstance();
    private BufferedWriter buffWriter;

    public KafkaBroker(String BootstrapServer) {
        if (BootstrapServer != null && !BootstrapServer.isEmpty()) {
            DEFAULT_BOOTSTRAP_SERVER = BootstrapServer;
        }
    }

    @Override
    public boolean Connect() {

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

    private Properties producerProps(String default_bootstrap_server) {
        String serializer = StringSerializer.class.getName();
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVER);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);
        return props;
    }

    @Override
    public boolean Publish(String topic, String key, String message) {
        try {
            long startTime = System.currentTimeMillis();
            mLogger.info("publish message:" + message + "for key:" + key + "on topic:" + topic);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
            producer.send(record, new Democallback(startTime, key, message));
            isPublished = true;
            return isPublished;
        }
        catch (Exception exc) {
            mLogger.info("msg is not published");
            isPublished = false;
            return isPublished;
        }
    }

    //democallback is called when the record sent to server is acknowledged
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
            mLogger.info("Called Callback method");

            if (metadata != null) {
                System.out.println(
                        "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                                "), " +
                                "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
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
    @Override
    public void runConsumer() throws InterruptedException {
        final int max = 2;
        int noRecordsCount = 0;


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            //mLogger.info("Found {} records in kafka", records.count());
            if (records.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > max) break;
                    // If no message found count is reached to threshold exit loop.
                else continue;
            }
            //print each record
            records.forEach(record -> {
                System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
                propapp.setProperty("FileName",record.value());
            });


        }
        try {
            consumer.commitSync();
            mLogger.info("finfished sync");
        } catch (CommitFailedException e) {
            mLogger.error("commit failed", e);
        }

    }

    public void run() throws InterruptedException {

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
            runConsumermultipleTopics();
        }
    }

    private void runConsumermultipleTopics() {
        final int max = 2;
        int noRecordsCount = 0;

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.count() == 0) {
                    noRecordsCount++;
                    if (noRecordsCount > max) break;
                        // If no message found count is reached to threshold exit loop.
                    else continue;
                }


                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        mLogger.info(record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();

                   // System.out.println(lastOffset);
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }


            }
            try{
            //commits the offset of record to broker.
            consumer.commitSync();
                mLogger.info("finished sync");

        } catch (CommitFailedException e) {
            mLogger.error("commit failed", e);
        }
    }




    @Override
    public boolean isConnected() {
        return isConnected;
    }


    private Properties consumerProps(String default_bootstrap_server, String groupid) {
        String deserializer = StringDeserializer.class.getName();
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupid);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }


    public void FileProducer( File file,String key, String topic) {

        propapp.setProperty("FileName", file.getName());

        String filename=propapp.getProperty("FileName");

        long startTime = System.currentTimeMillis();
        mLogger.info("publish message on file :"+filename+"\ton topic:"+topic);

        // Read each line from the file and send via the producer

        try(BufferedReader br = new BufferedReader(new FileReader(file)))

        {
            String line = br.readLine();

            while (line != null)
            {
                ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, key, line);
                producer.send(data,new FileDemocallback(startTime, key, line));
                //System.out.println(line);

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
            mLogger.info("Called Callback method");

            if (metadata != null) {
                System.out.println("message(" + key + ", " + message + ") sent to partition(" + metadata.partition()
                        + "), " + "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
            } else {
                exception.printStackTrace();
            }
        }

    }
    public void FileConsumer(String topic, String filePath) throws IOException {

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

        final int max = 2;
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
            mLogger.info("finished sync");
        }

        catch (CommitFailedException e)
        {
            mLogger.error("commit failed", e);
        }

    }
    @Override
    public void close() {
        try{

            if(producer!=null)
            {
                mLogger.info("Closing producer's connection");
                producer.close();
            }
            if(consumer!=null)
            {
                mLogger.info("Closing consumer's connection");
                consumer.close();
                System.out.println("DONE");
            }
        }
        catch(Exception ex)
        {
            mLogger.info("cannot close the connection");


        }
    }









}