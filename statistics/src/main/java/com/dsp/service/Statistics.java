package com.dsp.service;

import com.dsp.message.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Util;

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

        final Properties consumerProperties = Util.loadPropertiesFromFile(Statistics.CONSUMER_PROPERTIES);
        final Properties producerProperties = Util.loadPropertiesFromFile(Statistics.PRODUCER_PROPERTIES);
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
}
