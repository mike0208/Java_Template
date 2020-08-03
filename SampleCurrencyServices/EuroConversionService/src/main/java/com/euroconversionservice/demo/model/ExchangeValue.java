package com.euroconversionservice.demo.model;

public class ExchangeValue {

	private String currency;
	private int value;

	
	
	public ExchangeValue() {
	}

	public ExchangeValue(String currency, int value) {
		super();
		this.currency = currency;
		this.value = value;
	}

	public String getCurrency() {
		return currency;
	}

	public void setCurrency(String currency) {
		this.currency = currency;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

}
