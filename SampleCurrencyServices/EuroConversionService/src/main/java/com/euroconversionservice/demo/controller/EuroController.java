package com.euroconversionservice.demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.euroconversionservice.demo.model.ConversionEntity;
import com.euroconversionservice.demo.model.ExchangeValue;
import com.euroconversionservice.demo.service.EuroService;

@RestController
public class EuroController {

	@Autowired
	EuroService service;
	String currency = "EURO";

	@GetMapping("/")
	public String check() {
		return "its OK";
	}


	@GetMapping("EuroConversionUsingString/{quantity}")
	public ConversionEntity euroConversionUsingString(@PathVariable int quantity) {
		String data = service.consumeString();
		ConversionEntity entity = new ConversionEntity(currency, Integer.valueOf(data), quantity,
				quantity * Integer.valueOf(data));
		return entity;
	}

	@GetMapping("EuroConversionUsingFile/{quantity}")
	public ConversionEntity euroConversionUsingFile(@PathVariable int quantity) {
		ExchangeValue value = service.getExchangeValue();
		ConversionEntity entity = new ConversionEntity(currency, value.getValue(), quantity,
				quantity * value.getValue());
		return entity;
	}
}
