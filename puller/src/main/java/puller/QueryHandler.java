package puller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import message.BatchMessage;
import message.PropertyMessage;
import message.RequestMessage;
import model.PropertyData;
import model.Query;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import util.Util;

import java.time.LocalDate;
import java.util.*;

@RestController
public class QueryHandler {
    private KafkaProducer queryPublisher;

    public QueryHandler() {
        Properties props = Util.loadPropertiesFromFile("producer.properties");
        queryPublisher = new KafkaProducer(props);
    }

    //todo return http msg
    @RequestMapping(value = "/query", method = RequestMethod.POST)
    public void query(@RequestBody RequestMessage request) {
        // Get data needed to fulfill the query
        Query query = request.getQuery();
        Map<String, List<PropertyMessage>> tableNameToPropertyMessageMap = Puller.getQueryData(query);

        PropertyMessage[] messages = new PropertyMessage[5];
        for (int i = 0; i < 5; i++) {
            messages[i] = new PropertyMessage(
                    System.currentTimeMillis(), LocalDate.now(),
                    new PropertyData(query.getCounty(), query.getPropertyType(), query.getMinPrice(), query.getPostcodePrefix(), ImmutableMap.of())
            );
        }

        for (Map.Entry<String, List<PropertyMessage>> propertyMessages : tableNameToPropertyMessageMap.entrySet()) {
           BatchMessage batchMessage = new BatchMessage(request.getUuid(), request.getPartitionID(), System.currentTimeMillis(),
                    request.getQuery(), propertyMessages.getValue().toArray(new PropertyMessage[0]));

            try {
                sendPropertyData("requests_" + propertyMessages.getKey(), batchMessage);
            } catch (JsonProcessingException e) {
                System.err.println("Failed to publish request");
                e.printStackTrace();
            }
        }

    }


    /**
     * Send data to be processed
     */
    public void sendPropertyData(final String topic, final BatchMessage message) throws JsonProcessingException {
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

    /*
    @RequestMapping(value = "/query", method = RequestMethod.POST)
    public void query(@RequestBody RequestMessage request) {
        // Get data needed to fulfill the query
        Query query = request.getQuery();
        Map<String, List<PropertyData>> queryData = Puller.getQueryData(query);

        PropertyMessage[] messages = new PropertyMessage[5];
        for (int i = 0; i < 5; i++) {
            messages[i] = new PropertyMessage(
                    System.currentTimeMillis(), LocalDate.now(),
                    new PropertyData(query.getCounty(), query.getPropertyType(), query.getMinPrice(), query.getPostcodePrefix(), ImmutableMap.of())
            );
        }

        final BatchMessage batchMessage = new BatchMessage(request.getUuid(), request.getPartitionID(), System.currentTimeMillis(),
                request.getQuery(), messages);

        for (Map.Entry<String, List<PropertyData>> dataSet : queryData.entrySet()) {
            try {
                sendPropertyData("requests_" + dataSet.getKey(), dataSet.getValue());
            } catch (JsonProcessingException e) {
                System.err.println("Failed to publish request");
                e.printStackTrace();
            }
        }

    }

     */



}
