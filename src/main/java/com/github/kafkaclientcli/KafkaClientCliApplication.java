package com.github.kafkaclientcli;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;

@SpringBootApplication
public class KafkaClientCliApplication {

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(KafkaClientCliApplication.class, args)));
    }
    
    @PostConstruct
    void setUp(){
        System.setProperty("file.encoding","UTF-8");
    }

}
