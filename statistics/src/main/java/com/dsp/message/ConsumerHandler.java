package com.dsp.message;

import com.dsp.processing.StatisticsProcessor;
import message.BatchMessage;
import message.Message;
import message.MessageDeserializer;
import message.PropertyMessage;
import model.StatisticsResult;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * ConsumerHandler is responsible for polling periodically from the specified requests topic(which is set on startup).
 * If it finds a {@link org.apache.kafka.clients.consumer.ConsumerRecord} in the topic it then deserializes it, extracts
 * the necessary information such as the uuid, partitionID and {@link message.BatchMessage} and then processes the
 * {@link message.BatchMessage} asynchronously. Once the batch has been processed the results are put into the global
 * results map.
 */
public class ConsumerHandler extends KafkaHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerHandler.class);

    private final MessageDeserializer deserializer;
    private final Consumer<String, String> consumer;
    private final ExecutorService workers = Executors.newCachedThreadPool();

    public ConsumerHandler(final ConcurrentMap<Pair<UUID, Integer>, StatisticsResult[]> results,
                           final Properties consumerProperties, final String consumerTopic,
                           final MessageDeserializer deserializer) {
        super(results);
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.consumer.subscribe(Collections.singletonList(consumerTopic));
        this.deserializer = deserializer;
        LOGGER.debug(consumer.partitionsFor(consumerTopic).toString());
    }

    @Override
    public void run() {
        try {
            while (true) {
                LOGGER.debug("polling...");
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (!records.isEmpty()) {
                    LOGGER.info("Records consumed from kafka");
                }
                for (ConsumerRecord<String, String> record : records) {
                    final Message<PropertyMessage[]> message = deserializer.deserialize(record.value());
                    final int partitionID = ((BatchMessage) message).getPartitionID();
                    final UUID uuid = ((BatchMessage) message).getUuid();
                    final BatchMessage m = (BatchMessage) message;
                    LOGGER.info("Number of PropertyMessages batch: " + m.getData().length);
                    CompletableFuture.runAsync(() ->{
                        final long startTime = System.currentTimeMillis();
                        getResults().put(ImmutablePair.of(uuid, partitionID), new StatisticsProcessor(((BatchMessage) message).getQuery()).apply(message.getData()));
                        final long endTime = System.currentTimeMillis();
                        LOGGER.debug("Total execution time: " + (endTime - startTime));
                    }, workers);
                }
                try {
                    Thread.sleep(500);
                } catch (final InterruptedException ex) {
                    LOGGER.warn(ex.getMessage());
                }
            }
        } finally {
            consumer.close();
            LOGGER.info("Closing consumer");
        }
    }
}
