package com.euroconversionservice.demo.model;


public class ConversionEntity {
	
	String currency;
	int exchangeValue;
	int quantity;
	int totalCalculatedAmount;
	
	public ConversionEntity() {
	}

	
	public ConversionEntity(String currency, int exchangeValue, int quantity, int totalCalculatedAmount) {
		this.currency = currency;
		this.exchangeValue = exchangeValue;
		this.quantity = quantity;
		this.totalCalculatedAmount = totalCalculatedAmount;
	}

	public String getCurrency() {
		return currency;
	}

	public void setCurrency(String currency) {
		this.currency = currency;
	}

	public int getExchangeValue() {
		return exchangeValue;
	}

	public void setExchangeValue(int exchangeValue) {
		this.exchangeValue = exchangeValue;
	}

	public int getQuantity() {
		return quantity;
	}

	public void setQuantity(int quantity) {
		this.quantity = quantity;
	}

	public int getTotalCalculatedAmount() {
		return totalCalculatedAmount;
	}

	public void setTotalCalculatedAmount(int totalCalculatedAmount) {
		this.totalCalculatedAmount = totalCalculatedAmount;
	}
	
	
}
