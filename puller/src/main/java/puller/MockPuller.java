package puller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import kafka.KafkaConstants;
import message.BatchMessage;
import message.PropertyMessage;
import message.RequestMessage;
import model.PropertyData;
import model.Query;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import util.Util;

import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

@RestController
public class MockPuller {
    private KafkaProducer queryPublisher;
    private static String DEFAULT_HOST = "kafka:9093";

    @Value("${hostname}")
    private String hostname;

    public MockPuller() {
        System.out.println("Puller Created");
        if (hostname != null)
            System.out.println(hostname);
        else
            System.out.println("HOSTNAME is null");

        queryPublisher = initQueryPublisher();
    }

    @RequestMapping(value = "/query", method = RequestMethod.POST)
    public void query(@RequestBody RequestMessage request) {
        Query query = request.getQuery();
        PropertyMessage[] messages = new PropertyMessage[5];
        for (int i = 0; i < 5; i++) {
            messages[i] = new PropertyMessage(
                    System.currentTimeMillis(), LocalDate.now().minusMonths(i),
                    new PropertyData(query.getCounty(), query.getPropertyType(), query.getMinPrice(), query.getPostcodePrefix(), ImmutableMap.of())
            );
        }

        final BatchMessage batchMessage = new BatchMessage(request.getUuid(), request.getPartitionID(), System.currentTimeMillis(),
                request.getQuery(), messages);
        try {
            sendData(KafkaConstants.REQUESTS_DAFT, batchMessage);
        } catch (JsonProcessingException e) {
            System.err.println("Failed to publish request");
            e.printStackTrace();
        }

    }

    /**
     * Send data to be processed
     * @param message
     */
    public void sendData(final String topic, final BatchMessage message) throws JsonProcessingException {
        TestCallback callback = new TestCallback();
        queryPublisher.send(new ProducerRecord<>(topic, Util.objectMapper.writeValueAsString(message)), callback);
    }


    private static class TestCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.out.println("Error while producing message to topic :" + recordMetadata);
                e.printStackTrace();
            } else {
                String message = String.format("sent message to topic:%s partition:%s  offset:%s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                System.out.println(message);
            }
        }
    }

    private KafkaProducer initQueryPublisher() {
        Properties props = new Properties();

        if (StringUtils.isEmpty(hostname)) {
            props.setProperty("bootstrap.servers", DEFAULT_HOST);
        } else {
            props.setProperty("bootstrap.servers", hostname);
        }
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("max.message.bytes", "100000");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer(props);
    }
}
