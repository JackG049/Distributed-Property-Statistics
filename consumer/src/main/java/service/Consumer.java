package service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class Consumer {
    public static void main(final String[] args) {
        // For now just going to assume we have everything correct and that the user never gets anything wrong


        final Properties consumerProperties = loadPropertiesFromFile("consumer.properties");

        consumerProperties.setProperty("group.id", "_statistics");
        new Consumer(consumerProperties).poll();

    }

    private static Properties loadPropertiesFromFile(final String fileName) {
        final Properties properties = new Properties();

        try (final InputStream propertiesInputStream  = Consumer.class.getClassLoader().getResourceAsStream(fileName)) {

            properties.load(propertiesInputStream);


        } catch (final IOException ex) {

            ex.printStackTrace();
            /*
                Do we fail fully and fast or do we try to recover?
             */
        }
        return properties;
    }

    private final KafkaConsumer<UUID, String> kafkaConsumer;

    public Consumer(final Properties consumerProperties) {
        this.kafkaConsumer = new KafkaConsumer<UUID, String>(consumerProperties);
        kafkaConsumer.subscribe(Collections.singletonList("results"));
    }

    public void poll() {
        int i = 0;
        while (i < 64) {
//            System.out.println("polling");
            final ConsumerRecords<UUID, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (final ConsumerRecord<UUID, String> record : records) {
                System.out.println("\ti = " + i + "\tReceived offset: " + record.offset());
                i++;
            }

        }

    }
}
