package com.saurabh.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class WikimediaChangesConsumer {

    private static final Logger logger = LoggerFactory.getLogger(WikimediaChangesConsumer.class.getSimpleName());

    public static void main(String args[]) {
        logger.info("Consumer for wikimedia");
        String fileName = System.currentTimeMillis() + "consumerOutput.txt";
        Path filePath = Path.of(fileName);
        String groupId = "java-wikimedia-cosumer-1";
        String bootstrapServers = "127.0.0.1:9092";

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        Thread mainThread = Thread.currentThread();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        try {

            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    logger.info("Detected a shutdown, calling cosumer.wakeUp() method!!");
                    consumer.wakeup();

                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            // subscribe to a topic
            // consumer.subscribe(Arrays.asList("demo_java", "demo_java_callback", "demo_java_keys"));
            consumer.subscribe(Arrays.asList(Constants.TOPIC_NAME));
            Files.deleteIfExists(filePath);
            // Poll for data
            while (true) {
                // logger.info("Polling...");
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    // logger.info("Key: " + record.key() + " Value: " + record.value());
                    // logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
                    Files.writeString(filePath, record.value() + System.lineSeparator(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    Files.writeString(filePath, "=========================================" + System.lineSeparator(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);

                }
            }

        } catch (WakeupException e) {
            logger.info("Consumer is starting to shutdown!!");
        } catch (Exception e) {
            //logger.info("Unexpected exception!!");
            e.printStackTrace();
        } finally {
            consumer.close();
            logger.info("Consumer is shutdown");
        }


    }
}
