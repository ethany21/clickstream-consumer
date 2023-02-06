package com.github.ethany21.clickstreamconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class ClickstreamConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(ClickstreamConsumerApplication.class, args);
	}

}
