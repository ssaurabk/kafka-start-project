package com.saurabh.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());

    public static void main(String args[]) {
        logger.info("Hello World");
        String bootstrapServers = "127.0.0.1:9092";

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < 2000; i++) {
                //Thread.sleep(500);
                // Create a producer record
                String topic = "demo_java_consumer_groups";
                String key = "id_" + i;
                String message = "message-" + i + "-" + System.currentTimeMillis();
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, message);
                // Send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            logger.info("Key: " + key + " and message: " + message + " sent to topic: " + metadata.topic() + " partition: " + metadata.partition() + " with offset: " + metadata.offset() + " at " + metadata.timestamp());
                        } else {
                            logger.error(exception.getMessage());
                        }
                    }
                });
            }
            // flush and close the producer
            producer.flush();
            producer.close();

        }


    }
}
