package com.vnadkarni.tfl.tflkafka;

import java.lang.System;
import java.io.*;
import java.util.*;
import java.time.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import com.vnadkarni.tfl.tflkafka.avro.VehicleEventAvro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.ClassPathResource;

@Component
public class mainapp implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(mainapp.class);

    public static Properties readConfig(final String configFile) throws IOException {
        // reads the client configuration from application.properties
        // and returns it as a Properties object
        final Properties config = new Properties();
        try (InputStream inputStream = new ClassPathResource(configFile).getInputStream()) {
            config.load(inputStream);
        }

        return config;
    }

    /**
     * Produces a message to a Kafka topic.
     * 
     * Configures a Kafka producer with string serializers for both keys and values,
     * sends a single message to the specified topic, logs the operation, and closes
     * the producer connection.
     * 
     * @param topic  the name of the Kafka topic to produce the message to
     * @param config the producer configuration properties
     * @param key    the message key as a string
     * @param value  the message value as a string
     */
    public static void produce(String topic, Properties config, String key, String value) {
        // sets the message serializers
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // creates a new producer instance and sends a sample message to the topic
        Producer<String, String> producer = new KafkaProducer<>(config);
        producer.send(new ProducerRecord<>(topic, key, value));
        logger.info("Produced message to topic {}: key = {} value = {}", topic, key, value);

        // closes the producer connection
        producer.close();
    }

    public static void produce(String topic, Properties config, VehicleEventAvro event) {
        // sets the message serializers
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        // Ensure Schema Registry properties are available to the serializer
        if (!config.containsKey("schema.registry.url")) {
            logger.error("ERROR: schema.registry.url not found in configuration");
            logger.error("Available properties: {}", config.keySet());
            return;
        }

        // Debug: Log the schema registry URL to verify configuration
        logger.debug("Attempting to connect to Schema Registry: {}", config.getProperty("schema.registry.url"));

        // creates a new producer instance and sends a sample message to the topic
        Producer<String, VehicleEventAvro> producer = new KafkaProducer<>(config);
        try {
            logger.debug("Producing Avro message: key={}, type={}", event.getVehicleId(), event.getEventType());
            // Send the message and wait for acknowledgment
            RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, event.getVehicleId(), event)).get();
            logger.info("✓ Produced Avro message to topic {}: key = {} value = {} at offset {}", 
                    topic, event.getVehicleId(), event, metadata.offset());
        } catch (Exception e) {
            logger.error("✗ Failed to send Avro message: {}", e.getMessage());
            logger.error("This could indicate: Schema Registry unreachable, invalid credentials, or serialization failure", e);
        } finally {
            // Flush and close the producer connection
            producer.flush();
            producer.close();
        }
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info("Starting TFL Kafka Application");
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

        VehicleEventAvro event = new VehicleEventAvro("123", "Arrival", "Kings Cross", "2021-07-01T12:00:00");

        String topic = "tfl-vehicle-events";
        final Properties config = new Properties();

        try (InputStream inputStream = new ClassPathResource("application.properties").getInputStream()) {
            config.load(inputStream);
        } catch (IOException e) {
            logger.error("Failed to load application.properties", e);
        }

        while (true) {

            VehicleArrival[] response = null;
            try {
                response = builder.build()
                        .get()
                        .uri("https://api.tfl.gov.uk/Line/piccadilly/Arrivals")
                        .retrieve()
                        .bodyToMono(VehicleArrival[].class)
                        .block();
            } catch (Exception e) {
                logger.error("Failed to fetch vehicle arrivals: {}", e.getMessage(), e);
                continue;
            }
            
            if (response == null) {
                continue;
            }
            for (VehicleArrival vehicleArrival : response) {

                String oldLocation = vehicleLocationMap.get(vehicleArrival.getVehicleId());
                if (oldLocation == null) {
                    vehicleLocationMap.put(vehicleArrival.getVehicleId(), vehicleArrival.getCurrentLocation());
                } else {
                    if (!oldLocation.equals(vehicleArrival.getCurrentLocation())) {
                        logger.debug("Vehicle {} has changed location from {} to {}", vehicleArrival.getVehicleId(), oldLocation, vehicleArrival.getCurrentLocation());
                        // vehicleLocationMap.put(vehicleArrival.getVehicleId(),
                        // vehicleArrival.getCurrentLocation());
                        Scanner scanner = new Scanner(vehicleArrival.getCurrentLocation());
                        scanner.useDelimiter(" ");
                        if (scanner.hasNext()) {
                            String preposition = scanner.next();
                            if (preposition.equals("At")) {
                                String location = StringUtils.substringAfter(vehicleArrival.getCurrentLocation(),
                                        "At ");
                                if (!location.equals("Platform") && !oldLocation.equals(vehicleArrival.getCurrentLocation())) {
                                    vehicleLocationMap.put(vehicleArrival.getVehicleId(),
                                        vehicleArrival.getCurrentLocation());

                                    // Log vehicle arrival
                                    logger.info("Vehicle {} has arrived at {}", vehicleArrival.getVehicleId(), location);
                                    try {
                                        event = new VehicleEventAvro(vehicleArrival.getVehicleId(), "Arrival", location,
                                                LocalDateTime.now().toString());
                                        produce(topic, config, event);
                                    } catch (Exception exc) {
                                        logger.error("Error processing arrival event for vehicle {}", vehicleArrival.getVehicleId(), exc);
                                    }

                                }
                            } 
                            // else {
                                // String location = StringUtils.substringAfter(oldLocation, " ");
                                // if (!location.equals("Platform")) {
                                // // System.out
                                // // .println("Vehicle " + vehicleArrival.getVehicleId() + " has departed "
                                // // + location);
                                // try {
                                // event = new VehicleEventAvro(vehicleArrival.getVehicleId(), "Departure",
                                // location,
                                // LocalDateTime.now().toString());

                                // produce(topic, config, event);
                                // } catch (Exception e) {
                                // e.printStackTrace();
                                // }

                                // }
                            // }
                        }
                        scanner.close();
                    }
                }
            }
            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                logger.warn("Thread sleep interrupted", e);
            }
        }
    }
}
