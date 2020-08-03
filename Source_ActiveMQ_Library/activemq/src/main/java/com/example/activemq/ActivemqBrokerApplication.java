package com.example.activemq;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ActivemqBrokerApplication {
    public ActivemqBrokerApplication() {
    }

    public static void main(String[] args) {
        SpringApplication.run(ActivemqBrokerApplication.class, args);
    }
}