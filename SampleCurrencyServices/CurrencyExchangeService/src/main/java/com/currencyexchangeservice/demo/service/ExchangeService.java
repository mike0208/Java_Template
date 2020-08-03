package com.currencyexchangeservice.demo.service;

import java.io.File;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.msgs.kafka.Imessagebroker;
import com.msgs.kafka.KafkaBroker;

import LoggerLib.*;

@Service
public class ExchangeService {
	static slf4jWrapper mLogger = new slf4jWrapper("ConsoleBusMonitor.class");
	
	public ExchangeService() {
	}

	
	@Value("${key}")
	String key;
	@Value("${topic}")
	String topic;

	@Value("${euroValue}")
	String euroValue;

	@Value("${poundValue}")
	String poundValue;

	@Value("${usdValue}")
	String usdValue;

	@Value("${fileTopic}")
	String fileTopic;

	@Value("${euroFilePath}")
	String euroFilePath;
	
	@Value("${poundFilePath}")
	String poundFilePath;
	
	@Value("${usdFilePath}")
	String usdFilePath;

	public String publishString(String currency) {

		if (currency.equals("EURO")) {
			return publishExchangeValueUsingString(euroValue);
		}
		if (currency.equals("POUND")) {
			return publishExchangeValueUsingString(poundValue);
		}
		if (currency.equals("USD")) {
			return publishExchangeValueUsingString(usdValue);
		} else
			return "invalid currency";
	}

	public String publishFile(String currency) {

		if (currency.equals("EURO")) {
			return publishExchangeValueUsingFile(euroFilePath);
		}
		if (currency.equals("POUND")) {
			return publishExchangeValueUsingFile(poundFilePath);
		}
		if (currency.equals("USD")) {
			return publishExchangeValueUsingFile(usdFilePath);
		} else
			return "invalid currency";
	}

	public String publishExchangeValueUsingString(String value) {

		Imessagebroker msgbroker = new KafkaBroker("localhost:9092");
		msgbroker.Connect();
		boolean brokerconnection = msgbroker.isConnected();
		mLogger.Info("Is broker connected:" + brokerconnection); // put logger
		msgbroker.Publish(topic, key, value);
		msgbroker.close();
		mLogger.Info("Connection closed"); //logger
		return value;
	}

	public String publishExchangeValueUsingFile(String inputFilePath) {
		Imessagebroker msgbroker = new KafkaBroker("localhost:9092");
		msgbroker.Connect();
		boolean brokerconnection = msgbroker.isConnected();
		mLogger.Info("Is broker connected:" + brokerconnection); // put logger
		File file = new File(inputFilePath);
		msgbroker.Publish(fileTopic, key, file.getName());
		msgbroker.FileProducer(file, key, topic);
		msgbroker.close();
		mLogger.Info("Connection closed"); //logger
		return "File Published";
	}

	/*
	 * public ExchangeValueDTO getExchangeValue() { ObjectMapper objectMapper = new
	 * ObjectMapper(); ExchangeValueDTO exchangeValueDTO = new
	 * ExchangeValueDTO("USD", 76); //EURO 82 //USD 76 try {
	 * objectMapper.writeValue(new File("target/USD.json"), exchangeValueDTO); }
	 * catch (IOException e) { e.printStackTrace(); } return null; }
	 */
}
