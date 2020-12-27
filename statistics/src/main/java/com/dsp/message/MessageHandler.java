package com.dsp.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import message.Message;
import message.MessageDeserializer;
import message.MessageSerializer;
import model.StatisticsResult;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Util;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Main driver class of the Statistics Processor componenent. Utilizes composition rather than inheritance to allow both
 * the {@link com.dsp.message.ProducerHandler} and {@link com.dsp.message.ConsumerHandler} to exist within the same class.
 *
 * On instantiation, both {@link com.dsp.message.KafkaHandler}s are created and the shared results map passed to both of
 * them. Then in both are started in separate threads to allow concurrent processing.
 */
public final class MessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(Message.class);
    private static final ConcurrentMap<Pair<UUID, Integer>, StatisticsResult[]> results = new ConcurrentHashMap<>();

    private final KafkaHandler producer;
    private final KafkaHandler consumer;

    /**
     * Constructor for the MessageHandler
     * @param producerProperties The properties of the {@link org.apache.kafka.clients.producer.KafkaProducer}.
     * @param consumerProperties The properties of the {@link org.apache.kafka.clients.consumer.KafkaConsumer}.
     * @param consumerTopic The Kafka topic which to subscribe the {@link org.apache.kafka.clients.consumer.KafkaConsumer} to.
     */
    public MessageHandler(final Properties producerProperties,
                          final Properties consumerProperties,
                          final String consumerTopic) {
        final ObjectMapper mapper = Util.objectMapper;
        this.consumer = new ConsumerHandler(results, consumerProperties, consumerTopic, new MessageDeserializer(mapper));
        this.producer = new ProducerHandler(results, producerProperties, new MessageSerializer(mapper));
    }

    public void startHandlers() {
        LOGGER.info("Starting threads");
        Thread producerThread = new Thread(producer);

        LOGGER.info("Starting Producer");
        producerThread.start();
        Thread consumerThread = new Thread(consumer);
        LOGGER.info("Starting Consumer");
        consumerThread.start();

        try {
            producerThread.join();
            consumerThread.join();
        } catch (final InterruptedException ex) {
            LOGGER.warn(ex.getMessage());
        }

    }
}
