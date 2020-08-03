package com.currencyexchangeservice.demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.currencyexchangeservice.demo.service.ExchangeService;


@RestController
public class CurrencyExchangeController {

	@Autowired
	private ExchangeService exchangeService;
	
	@GetMapping("/")
	public String Check() {
		return "its working";
	}

	@GetMapping("exchangeUsingString/{currency}")
	public String exchangeString(@PathVariable String currency) {
		String message=exchangeService.publishString(currency);
		return "message published for: " + currency + " : " + message;
	}
	
	@GetMapping("exchangeUsingFile/{currency}")
	public String exchangeFile(@PathVariable String currency) {
		System.out.println(exchangeService.publishFile(currency));
		return "file published for: " + currency;
	}

	
	
	
	/*
	 * @GetMapping("exchangeValueDTO/{currency}") public String
	 * exchangeFile(@PathVariable String currency) {
	 * exchangeService.getExchangeValue(); return "file generated"; }
	 */
	
}
