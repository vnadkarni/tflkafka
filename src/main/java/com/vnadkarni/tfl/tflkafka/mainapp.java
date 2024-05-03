package com.vnadkarni.tfl.tflkafka;

import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.lang.Thread;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import org.apache.commons.lang3.StringUtils;

@Component
public class mainapp implements CommandLineRunner {
    @Override
    public void run(String[] args) throws Exception {
        System.out.println("Hello World");
        main(args);
    }

    public static void main(String[] args) {
        // Logic
        int bufferSize = 1 * 1024 * 1024; // 16MB
        HashMap<String, String> vehicleLocationMap = new HashMap<String, String>();
        ExchangeStrategies exchangeStrategies = ExchangeStrategies.builder()
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(bufferSize))
                .build();

        WebClient.Builder builder = WebClient.builder().exchangeStrategies(exchangeStrategies);

        String stringresponse = builder.build()
                .get()
                .uri("https://api.tfl.gov.uk/Line/piccadilly/Arrivals")
                .retrieve()
                .bodyToMono(String.class)
                .block();

        VehicleEvent event = new VehicleEvent("123", "Arrival", "Kings Cross", "2021-07-01T12:00:00");

        // System.out.println(stringresponse);

        while (true) {

            VehicleArrival[] response = builder.build()
                    .get()
                    .uri("https://api.tfl.gov.uk/Line/piccadilly/Arrivals")
                    .retrieve()
                    .bodyToMono(VehicleArrival[].class)
                    .block();
            for (VehicleArrival vehicleArrival : response) {

                // System.out.println(vehicleArrival.getVehicleId() + " " +
                // vehicleArrival.getCurrentLocation());

                String oldLocation = vehicleLocationMap.get(vehicleArrival.getVehicleId());
                if (oldLocation == null) {
                    vehicleLocationMap.put(vehicleArrival.getVehicleId(), vehicleArrival.getCurrentLocation());
                } else {
                    if (!oldLocation.equals(vehicleArrival.getCurrentLocation())) {
                        vehicleLocationMap.put(vehicleArrival.getVehicleId(), vehicleArrival.getCurrentLocation());
                        // System.out.println(vehicleArrival.getVehicleId() + " " +
                        // vehicleArrival.getCurrentLocation());
                        Scanner scanner = new Scanner(vehicleArrival.getCurrentLocation());
                        scanner.useDelimiter(" ");
                        if (scanner.hasNext()) {
                            String preposition = scanner.next();
                            if (preposition.equals("At")) {
                                String location = StringUtils.substringAfter(vehicleArrival.getCurrentLocation(),
                                        "At ");
                                if (!location.equals("Platform")) {
                                    System.out.println(
                                            "Vehicle " + vehicleArrival.getVehicleId() + " has arrived at " + location);
                                }
                            } else {
                                String location = StringUtils.substringAfter(oldLocation, " ");
                                if (!location.equals("Platform")) {
                                    System.out
                                            .println("Vehicle " + vehicleArrival.getVehicleId() + " has departed "
                                                    + location);
                                }
                            }
                        }
                        scanner.close();
                    }
                }
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
