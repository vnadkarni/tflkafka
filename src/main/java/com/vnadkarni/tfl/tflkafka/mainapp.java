package com.vnadkarni.tfl.tflkafka;

import java.lang.System;
import java.io.*;
import java.util.*;
import java.time.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import com.vnadkarni.tfl.tflkafka.avro.VehicleEventAvro;

import java.lang.Thread;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.ClassPathResource;

@Component
public class mainapp implements CommandLineRunner {

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
     * @param topic the name of the Kafka topic to produce the message to
     * @param config the producer configuration properties
     * @param key the message key as a string
     * @param value the message value as a string
     */
    public static void produce(String topic, Properties config, String key, String value) {
        // sets the message serializers
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // creates a new producer instance and sends a sample message to the topic
        Producer<String, String> producer = new KafkaProducer<>(config);
        producer.send(new ProducerRecord<>(topic, key, value));
        System.out.println(
                String.format(
                        "Produced message to topic %s: key = %s value = %s", topic, key, value));

        // closes the producer connection
        producer.close();
    }

        public static void produce(String topic, Properties config, VehicleEventAvro event) {
        // sets the message serializers
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        // Ensure Schema Registry properties are available to the serializer
        if (!config.containsKey("schema.registry.url")) {
            System.err.println("ERROR: schema.registry.url not found in configuration");
            System.err.println("Available properties: " + config.keySet());
            return;
        }

        // Debug: Log the schema registry URL to verify configuration
        System.out.println("Attempting to connect to Schema Registry: " + config.getProperty("schema.registry.url"));

        // creates a new producer instance and sends a sample message to the topic
        Producer<String, VehicleEventAvro> producer = new KafkaProducer<>(config);
        try {
            System.out.println("Producing Avro message: key=" + event.getVehicleId() + ", type=" + event.getEventType());
            // Send the message and wait for acknowledgment
            RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, event.getVehicleId(), event)).get();
            System.out.println(
                    String.format(
                    "✓ Produced Avro message to topic %s: key = %s value = %s at offset %d", 
                    topic, event.getVehicleId(), event, metadata.offset()));
        } catch (Exception e) {
            System.err.println("✗ Failed to send Avro message: " + e.getMessage());
            System.err.println("This could indicate: Schema Registry unreachable, invalid credentials, or serialization failure");
            e.printStackTrace();
        } finally {
            // Flush and close the producer connection
            producer.flush();
            producer.close();
        }
    }


    @Override
    public void run(String... args) throws Exception {
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

        VehicleEventAvro event = new VehicleEventAvro("123", "Arrival", "Kings Cross", "2021-07-01T12:00:00");

        String topic = "tfl-vehicle-events";
        final Properties config = new Properties();

        try (InputStream inputStream = new ClassPathResource("application.properties").getInputStream()) {
            config.load(inputStream);
        } catch (IOException e) {
            System.err.println("Failed to load application.properties");
            e.printStackTrace();
        }

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
                                    try {
                                        event = new VehicleEventAvro(vehicleArrival.getVehicleId(), "Arrival", location,
                                                LocalDateTime.now().toString());
                                        produce(topic, config, event);
                                    } catch (Exception exc) {
                                        exc.printStackTrace();
                                    }

                                }
                            } else {
                                String location = StringUtils.substringAfter(oldLocation, " ");
                                if (!location.equals("Platform")) {
                                    System.out
                                            .println("Vehicle " + vehicleArrival.getVehicleId() + " has departed "
                                                    + location);
                                    try {
                                        event = new VehicleEventAvro(vehicleArrival.getVehicleId(), "Departure", location,
                                                LocalDateTime.now().toString());

                                        produce(topic, config, event);
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }

                                }
                            }
                        }
                        scanner.close();
                    }
                }
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
