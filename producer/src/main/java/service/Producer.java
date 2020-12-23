package service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import message.BatchMessage;
import message.PropertyMessage;
import model.PropertyData;
import model.Query;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import util.Util;

import java.time.LocalDate;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Producer {

    public static void main(final String[] args)  {
        Producer producer = new Producer("requests_myhome");
        Producer producer1 = new Producer("requests_daft");
        ExecutorService service = Executors.newFixedThreadPool(2);
        final PropertyMessage[] messages = new PropertyMessage[5000];
        final String[] counties = {"Antrim",
                "Armagh",
                "Carlow",
                "Cavan",
                "Clare",
                "Cork",
                "Derry",
                "Donegal",
                "Down",
                "Dublin",
                "Fermanagh",
                "Galway",
                "Kerry",
                "Kildare",
                "Kilkenny",
                "Laois",
                "Leitrim",
                "Limerick",
                "Longford",
                "Louth",
                "Mayo",
                "Meath",
                "Monaghan",
                "Offaly",
                "Roscommon",
                "Sligo",
                "Tipperary",
                "Tyrone",
                "Waterford",
                "Westmeath",
                "Wexford",
                "Wicklow"};
        service.submit(() -> {
            for (final String county : counties) {

                for (int i = 0; i < 5000; i++) {
                    messages[i] = new PropertyMessage(
                            System.currentTimeMillis(), LocalDate.now(), new PropertyData(county, "house", 1000.0, "D18", ImmutableMap.of())
                    );
                }
                final BatchMessage batchMessage = new BatchMessage(UUID.randomUUID(), 0, System.currentTimeMillis(), new Query(county, "house", null, null, null), messages);
                try {
                    producer.send(batchMessage);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
            producer.close();
        });

        service.submit(() -> {
            for (final String county : counties) {

                for (int i = 0; i < 5000; i++) {
                    messages[i] = new PropertyMessage(
                            System.currentTimeMillis(), LocalDate.now(), new PropertyData(county, "house", 1000.0, "D18", ImmutableMap.of())
                    );
                }
                final BatchMessage batchMessage = new BatchMessage(UUID.randomUUID(), 0, System.currentTimeMillis(), new Query(county, "house", null, null, null), messages);
                try {
                    producer1.send(batchMessage);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }

            }
            producer1.close();
        });
    }

    private final String topic;
    private final org.apache.kafka.clients.producer.Producer<String, String> producer;

    public Producer(final String topic) {
        this.topic = topic;
        Properties props = new Properties();
        // Don't quote me on these, they are tmp and copy pasted
        props.setProperty("bootstrap.servers", "kafka:9093");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("max.message.bytes", "100000");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<>(props);
    }

    public void send(final BatchMessage message) throws JsonProcessingException {
        TestCallback callback = new TestCallback();
        producer.send(new ProducerRecord<>(topic, Util.objectMapper.writeValueAsString(message)), callback);
    }

    public void close() {
        producer.close();
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
}