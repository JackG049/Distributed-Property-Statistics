package com.dsp.message;

import com.google.common.base.Preconditions;
import kafka.KafkaConstants;
import message.MessageSerializer;
import message.ResultsMessage;
import model.StatisticsResult;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

public class PublishHandler extends KafkaHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(PublishHandler.class);

    private final MessageSerializer serializer;
    private final Producer<UUID, String> producer;

    public PublishHandler(final ConcurrentMap<Pair<UUID, Integer>, StatisticsResult[]> results,
                          final Properties producerProperties, final MessageSerializer serializer) {
        super(results);
        this.producer = new KafkaProducer<>(producerProperties);
        this.serializer = Preconditions.checkNotNull(serializer, "serializer must not be null");
    }

    @Override
    public void run() {
        try {
            while (true) {
                LOGGER.debug("Results size - " + getResults().size());
                if (hasResults()) {
                    LOGGER.info(getResults().size() + " Result(s) found , publishing to kafka");

                    synchronized (getResults()) {
                        LOGGER.info("Preparing to publish results to Kafka");
                        for (final Map.Entry<Pair<UUID, Integer>, StatisticsResult[]> result : getResults().entrySet()) {
                            final UUID uuid = result.getKey().getKey();
                            final int partitionId = result.getKey().getValue();
                            final ResultsMessage message = new ResultsMessage(uuid, partitionId, System.currentTimeMillis(), result.getValue());
                            final ProducerRecord<UUID, String> record =
                                    new ProducerRecord<>(KafkaConstants.RESULTS, partitionId, uuid,serializer.serialize(message));
                            LOGGER.info("partitionID: "+partitionId);
                            producer.send(record, (metadata, e) -> {
                                if(e != null) {
                                    e.printStackTrace();
                                } else {
                                    LOGGER.info("Published record to Kafka at offset: " + metadata.offset());
                                }
                            });
                        }
                        getResults().clear();
                    }
                } else {
                    try {
                        Thread.sleep(1000);
                    } catch (final InterruptedException ex) {
                        LOGGER.warn(ex.getMessage());
                    }
                }
            }
        } finally {
            LOGGER.info("Closing producer");
            producer.close();
        }
    }
}
