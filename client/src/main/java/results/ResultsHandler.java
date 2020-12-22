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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@Getter
public class ResultsHandler implements Runnable {
    private final Map<Pair<UUID, Integer>,StatisticsResult[]> map = new HashMap<Pair<UUID, Integer>,StatisticsResult[]>();
    private static final Logger LOGGER = LoggerFactory.getLogger(ResultsHandler.class);

    private final MessageDeserializer deserializer;
    private final Consumer<String, String> consumer;
    private final int partitionId;

    public ResultsHandler(final Properties consumerProperties, final MessageDeserializer deserializer) {
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.deserializer = Preconditions.checkNotNull(deserializer, "serializer must not be null");
        consumer.subscribe(Collections.singletonList(KafkaConstants.RESULTS));
        this.partitionId = new ArrayList<>(consumer.assignment()).get(0).partition();
    }

    @Override
    public void run() {
        try {
            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (!records.isEmpty()) {
                    LOGGER.info("Records consumed from kafka");
                }
                for (ConsumerRecord<String, String> record : records) {
                    final Message<StatisticsResult[]> message = deserializer.deserialize(record.value());
                    final int partitionID = ((ResultsMessage) message).getPartitionID();
                    final StatisticsResult[] data =  ((ResultsMessage) message).getData();
                    final UUID uuid = ((ResultsMessage) message).getUuid();
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
        return this.map.containsKey(Pair.of(uuid, partitionId)) ? true : false;
    }

    public StatisticsResult[] getResult(UUID uuid) {
        return this.map.get(Pair.of(uuid,partitionId));
    }
}
