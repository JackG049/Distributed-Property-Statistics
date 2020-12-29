package puller;

import com.fasterxml.jackson.core.JsonProcessingException;
import message.BatchMessage;
import message.PropertyMessage;
import message.RequestMessage;
import model.Query;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import util.Util;

import java.util.*;

/**
 * QueryHandler is the main class in the puller package -- all data must ingress and egress through this class. This class accepts REST query requests from the client;
 * it retrieves data that is relevant to the query from a database of historical property information; and it publishes
 * the query and the query data to Kafka so that it can be processed.
 */

@RestController
public class QueryHandler {
    private final KafkaProducer queryPublisher;

    public QueryHandler() {
        Properties props = Util.loadPropertiesFromFile("producer.properties");
        queryPublisher = new KafkaProducer(props);
    }

    /**
     * Accepts query requests via REST. It collects data relevant to the query and sends it.
     * @param request which contains a query and meta data
     * @return
     */
    @RequestMapping(value = "/query", method = RequestMethod.POST)
    public ResponseEntity<String> query(@RequestBody RequestMessage request) {
        // Get data needed to fulfill the query
        Query query = request.getQuery();
        Map<String, List<PropertyMessage>> tableNameToPropertyMessageMap = Puller.getQueryData(query);

        // Package and send each query and its relevant data
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

        return new ResponseEntity<>("Query Acknowledged.", HttpStatus.OK);
    }

    /**
     * Publishes property queries and related data to Kafka so it can be processed.
     * @param topic is the kafka topic where the message will be sent
     * @param message contains a property query and related data
     * @throws JsonProcessingException
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

}
