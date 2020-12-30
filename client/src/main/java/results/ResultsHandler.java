package results;

import com.google.common.base.Preconditions;
import kafka.KafkaConstants;
import lombok.Getter;
import message.Message;
import message.MessageDeserializer;
import message.ResultsMessage;
import model.StatisticsResult;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@Getter
public class ResultsHandler implements Runnable {
    private final Map<Pair<UUID, Integer>,StatisticsResult[]> map = new HashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(ResultsHandler.class);

    private final MessageDeserializer deserializer;
    private final Consumer<UUID, String> consumer;
    private int partitionId;

    /**
     * Create new Kafka consumer subscribed to topic "Results"
     * @param consumerProperties
     * @param deserializer
     */
    public ResultsHandler(final Properties consumerProperties, final MessageDeserializer deserializer) {
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.deserializer = Preconditions.checkNotNull(deserializer, "serializer must not be null");
        consumer.subscribe(Collections.singletonList(KafkaConstants.RESULTS), new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(final Collection<TopicPartition> collection) {
                LOGGER.info("Partitions revoked for results consumer");
            }

            @Override
            public void onPartitionsAssigned(final Collection<TopicPartition> collection) {
                LOGGER.info("Got assigned partitions: " + collection);
                partitionId = new ArrayList<>(consumer.assignment()).get(0).partition();
                LOGGER.info("Assigned Partition " + partitionId);
            }
        });
        consumer.poll(Duration.ofMillis(1000));
    }

    @Override
    public void run() {
        try {
            while (true) {
                LOGGER.debug("polling...");
                final ConsumerRecords<UUID, String> records = consumer.poll(Duration.ofMillis(1000));
                if (!records.isEmpty()) {
                    LOGGER.info("Records consumed from kafka");
                }
                for (ConsumerRecord<UUID, String> record : records) {
                    final Message<StatisticsResult[]> message = deserializer.deserialize(record.value());
                    final ResultsMessage resultsMessage = (ResultsMessage) message;
                    final int partitionID = resultsMessage.getPartitionID();
                    final StatisticsResult[] data =  resultsMessage.getData();
                    final UUID uuid = resultsMessage.getUuid();
                    map.put(Pair.of(uuid, partitionID), data);
                }
                try {
                    Thread.sleep(500);
                } catch (final InterruptedException e) {
                    LOGGER.warn(e.getMessage());
                }
            }
        } finally {
            consumer.close();
            LOGGER.info("Closing consumer");
        }
    }

    public boolean isEmpty(UUID uuid) {
        return this.map.containsKey(Pair.of(uuid, partitionId)) ? false : true;
    }

    public StatisticsResult[] getResult(UUID uuid) {
        return this.map.get(Pair.of(uuid,partitionId));
    }
}
