package com.saurabh.demo;

import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.launchdarkly.eventsource.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    private static final Logger logger = LoggerFactory.getLogger(WikimediaChangesProducer.class.getSimpleName());

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "wikimedia.recentChanges";
        createNewTopic(topic, bootstrapServers);

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // High throughput producer configs
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");


        // Create the producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
            String url = "https://stream.wikimedia.org/v2/stream/recentchange";
            EventSource source = new EventSource.Builder(eventHandler, URI.create(url)).build();
            source.start();

            // lock the thread for 10 mins.
            TimeUnit.MINUTES.sleep(10);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void createNewTopic(String topicName, String bootstrapServers) {
        int partitions = 3;
        short replicationFactor = 1;

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(config)) {
            // Check if topic exists
            boolean topicExists = admin.listTopics().names().get().contains(topicName);

            while (topicExists) {
                logger.info("Topic exists. Deleting: " + topicName);
                DeleteTopicsResult deleteTopicsResult = admin.deleteTopics(Collections.singleton(topicName));
                deleteTopicsResult.all().get(); // Wait for deletion
                logger.info("Deleted topic: " + topicName);

                // Optional: Wait a bit to ensure deletion is fully processed
                Thread.sleep(2000);
                topicExists = admin.listTopics().names().get().contains(topicName);
            }

            // Create new topic
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            admin.createTopics(Collections.singleton(newTopic)).all().get();
            logger.info("Created topic: " + topicName);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error while deleting topic or creating topic!!", e);
        }
    }

}
