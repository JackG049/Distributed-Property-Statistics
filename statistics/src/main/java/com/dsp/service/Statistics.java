package com.dsp.service;

import com.dsp.message.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Statistics {
    private static final Logger LOGGER = LoggerFactory.getLogger(Statistics.class);
    private static final int SUPPORTED_NUM_ARGS = 2;
    private static final String CONSUMER_PROPERTIES = "consumer.properties";
    private static final String PRODUCER_PROPERTIES = "producer.properties";

    public static void main(final String[] args) {
        // For now just going to assume we have everything correct and that the user never gets anything wrong
        String topic = null;
        String host = null;
        if (args.length == SUPPORTED_NUM_ARGS) {
            host = args[0];
            topic = args[1];
        } else if (args.length > SUPPORTED_NUM_ARGS || args.length == 0) {
            LOGGER.error("Wrong number of arguments, exiting");
            System.exit(1);
        } else {
            topic = args[0];
        }

        final Properties consumerProperties = loadPropertiesFromFile(Statistics.CONSUMER_PROPERTIES);
        final Properties producerProperties = loadPropertiesFromFile(Statistics.PRODUCER_PROPERTIES);
        if (host != null) {
            LOGGER.info("Host argument passed, using user-specified value");
            consumerProperties.setProperty("bootstrap.servers", host);
            producerProperties.setProperty("bootstrap.servers", host);
        }
        consumerProperties.setProperty("group.id", topic + "_statistics");
        LOGGER.debug("Consumer Group = " + topic + "_statistics");
        final MessageHandler messageHandler = new MessageHandler(producerProperties, consumerProperties, topic);
        messageHandler.startHandlers();
    }

    private static Properties loadPropertiesFromFile(final String fileName) {
        final Properties properties = new Properties();

        try (final InputStream propertiesInputStream = Statistics.class.getClassLoader().getResourceAsStream(fileName)) {
            LOGGER.info("Attempting to load properties from " + fileName + "...");
            properties.load(propertiesInputStream);
            LOGGER.info("Success");

        } catch (final IOException ex) {
            LOGGER.warn("Failure");
            ex.printStackTrace();
            /*
                Do we fail fully and fast or do we try to recover?
             */
        }
        return properties;
    }
}
