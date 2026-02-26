package com.vnadkarni.tfl.tflkafka;

import java.lang.System;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.time.*;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.reactive.function.client.WebClient;

@SpringBootApplication
public class TflkafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(TflkafkaApplication.class, args);
	}

}
