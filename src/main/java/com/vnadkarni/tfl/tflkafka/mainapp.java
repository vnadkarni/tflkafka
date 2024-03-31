package com.vnadkarni.tfl.tflkafka;

import java.util.List;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

@Component
public class mainapp implements CommandLineRunner{

    @Override
    public  void run (String[] args) throws Exception {
    System.out.println("Hello World");
        main(args);
    }

    public static void main(String[] args) {
        // Logic
        int bufferSize = 1 * 1024 * 1024; // 16MB
        ExchangeStrategies exchangeStrategies = ExchangeStrategies.builder()
    .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(bufferSize))
    .build();

    WebClient.Builder builder = WebClient.builder().exchangeStrategies(exchangeStrategies);

        VehicleArrival[] response = builder.build()
        .get()
        .uri("https://api.tfl.gov.uk/Line/piccadilly/Arrivals")
        .retrieve()
        .bodyToMono(VehicleArrival[].class)
        .block();
        System.out.println(response[0].$type);
    }    
}
