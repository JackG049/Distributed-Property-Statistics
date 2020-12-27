package puller;

import com.google.common.base.Preconditions;
import kafka.KafkaConstants;
import lombok.Getter;
import message.BatchMessage;
import message.MessageSerializer;

import java.util.UUID;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
public class QueryPublisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryPublisher.class);

    private final MessageSerializer serializer;
    private final Producer<UUID, String> producer;

    public QueryPublisher(final MessageSerializer serializer) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "0.0.0.0:9093");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("max.message.bytes", "100000");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(props);
        this.serializer = Preconditions.checkNotNull(serializer, "serializer must not be null");
    }


    // todo constants
    public void publish(BatchMessage message) {
        try {
            LOGGER.info("Preparing to publish results to Kafka");
            final ProducerRecord<UUID, String> record =
                    new ProducerRecord<>(KafkaConstants.REQUESTS_MYHOME, message.getPartitionID(), message.getUuid(), serializer.serialize(message));
            producer.send(record, (metadata, e) -> {
                if(e != null) {
                    e.printStackTrace();
                } else {
                    LOGGER.info("Published record to Kafka at offset: " + metadata.offset());
                }
            });
        } finally {
            LOGGER.info("Closing producer");
            producer.close();
        }
    }

}
