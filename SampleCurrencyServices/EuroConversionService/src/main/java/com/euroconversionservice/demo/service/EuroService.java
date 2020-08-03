package com.euroconversionservice.demo.service;

import java.io.File;
import java.io.IOException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import LoggerLib.*;

import com.euroconversionservice.demo.model.ExchangeValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msgs.kafka.Imessagebroker;
import com.msgs.kafka.KafkaBroker;

@Service
public class EuroService {
	
	static slf4jWrapper mLogger = new slf4jWrapper("EuroService.class");

	@Value("${topic}")
	String topic;

	@Value("${fileTopic}")
	String fileTopic;

	@Value("${outputFilePath}")
	String outputFilePath;
	
	
	
	public String consumeString() {
		System.out.println(topic);

		System.out.println("kafka consumer");
		Imessagebroker msgbroker = new KafkaBroker("localhost:9092");
		try {
			String message = msgbroker.Subscribe(topic);
			mLogger.Info("message is : " + message); //logger
			msgbroker.close();
			return message;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return "wrong";
	}

	public String consumeFile() {
		Imessagebroker msgbroker = (Imessagebroker) new KafkaBroker("localhost:9092");

		System.out.println("Subscribed to topic:" + topic); //logger

		try {
			String fileName = msgbroker.Subscribe(fileTopic);
			msgbroker.FileConsumer(topic, outputFilePath);
			msgbroker.close();
			mLogger.Info("filename is : " + fileName); //logger
			String filePath=outputFilePath + fileName;
			mLogger.Info("filepath is : " + filePath);
			return filePath;
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return "error";
	}

	public ExchangeValue getExchangeValue() {
		
		String filePath = consumeFile();
		ExchangeValue exchangeValue=new ExchangeValue("default",0);

		ObjectMapper objectMapper = new ObjectMapper();
		try {
			exchangeValue = objectMapper.readValue(new File(filePath), ExchangeValue.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return exchangeValue;
	}

}
