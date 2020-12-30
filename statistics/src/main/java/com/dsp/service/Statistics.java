package com.dsp.service;

import com.dsp.message.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Util;

import java.util.Properties;

/**
 * Entry point to the Statistics Processing module. The Kafka host and the Kafka topic are passed to the program as
 * command line arguments and using these alongside properties which are loaded in the {@link com.dsp.message.MessageHandler}
 * is created and the processes started.
 */
public class Statistics {
    public static String TOPIC = "not_set_yet";

    private static final Logger LOGGER = LoggerFactory.getLogger(Statistics.class);
    private static final String CONSUMER_PROPERTIES = "consumer.properties";
    private static final String PRODUCER_PROPERTIES = "producer.properties";
    private static final int SUPPORTED_NUM_ARGS = 2;

    public static void main(final String[] args) {
        String host = null;
        if (args.length == SUPPORTED_NUM_ARGS) {
            host = args[0];
            TOPIC = args[1];
        } else if (args.length > SUPPORTED_NUM_ARGS || args.length == 0) {
            LOGGER.error("Wrong number of arguments, exiting");
            System.exit(1);
        } else {
            TOPIC = args[0];
        }

        final Properties consumerProperties = Util.loadPropertiesFromFile(Statistics.CONSUMER_PROPERTIES);
        final Properties producerProperties = Util.loadPropertiesFromFile(Statistics.PRODUCER_PROPERTIES);
        if (host != null) {
            LOGGER.info("Host argument passed, using user-specified value");
            consumerProperties.setProperty("bootstrap.servers", host);
            producerProperties.setProperty("bootstrap.servers", host);
        }

        // Sets the group.id for Kafka and enables the Consumer group functionality.
        consumerProperties.setProperty("group.id", TOPIC + "_statistics");
        LOGGER.debug("Consumer Group = " + TOPIC + "_statistics");
        final MessageHandler messageHandler = new MessageHandler(producerProperties, consumerProperties, TOPIC);
        messageHandler.startHandlers();
    }
}
