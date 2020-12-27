package puller;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Sets;
import kafka.KafkaConstants;
import message.BatchMessage;
import message.PropertyMessage;
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

import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

public class Puller {
    private final PropertyDbWrapper databaseWrapper = new PropertyDbWrapper();
    private final MockDataSource mockDataSource = new MockDataSource();
    private KafkaProducer queryPublisher;
    private Set<String> tableNames;

    public Puller() {
        queryPublisher = initQueryPublisher();

        Set<String> tables = Set.of("daft_ie");
        tableNames = databaseWrapper.getTableNames();
        Set<String> absentTables = Sets.difference(tables, tableNames);

        for (String absentTable : absentTables) {
            try {
                databaseWrapper.createPropertyTable(absentTable);
                tableNames.add(absentTable);
            } catch (InterruptedException e) {
                System.out.println("Failed to create table " + absentTable);
                e.printStackTrace();
            }
        }
    }

    //todo return http msg
    /**
     * Main puller method. Accepts queries, pull data, sends data to processors
     */
    //@RequestMapping(value = "/query", method = RequestMethod.POST)
    public void query(@RequestBody Query query) { //request message todo
        List<ItemCollection<QueryOutcome>> rawQueryData = new ArrayList<>();

        //for (String tableName : tableNames) {
            String tableName = "daft";
            String latestEntryDate = getLatestDatabaseEntry(tableName, query);
            PropertyMessage[] newPropertyListings = pullNewDataFromSource(tableName, latestEntryDate);
            //updateDatabase(tableName, newPropertyListings);
            //rawQueryData.add(queryDatabase(tableName, query));

            BatchMessage message = packageData(query, rawQueryData);
            try {
                sendData("requests_" + tableName, message);
            } catch (JsonProcessingException e) {
                System.err.println("Failed to publish request");
                e.printStackTrace();
            }
        //}

    }

    /**
     * Retrieve all relevant data from the database
     */
    public ItemCollection<QueryOutcome> queryDatabase(String tableName, Query query) {
        String periodStart = query.getStartDate();
        String periodEnd = query.getEndDate();
        String county = query.getCounty();


        ItemCollection<QueryOutcome> result;
        if (query.getMinPrice() != null && query.getMaxPrice() != null) {
            // todo add price range query
            result = databaseWrapper.queryTable(tableName, periodStart, periodEnd, county);
        } else {
            result = databaseWrapper.queryTable(tableName, periodStart, periodEnd, county);
        }

        return result;
    }

    /**
     * Bundle up data that needs to be processed
     */
    public BatchMessage packageData(Query query, List<ItemCollection<QueryOutcome>> rawQueryData) {
        UUID uuid = UUID.randomUUID(); //client
        int partitionId = 0; //client
        long timestamp = Instant.now().toEpochMilli(); //client
        List<PropertyMessage> data = new ArrayList<>();

        for (ItemCollection<QueryOutcome> queryResults : rawQueryData) {
            for (Item queryResult : queryResults) {
                LocalDate listingDate = LocalDate.parse(queryResult.getString("ListingDate"));
                PropertyData propertyData = propertyItemToPropertyData(queryResult);
                data.add(new PropertyMessage(timestamp, listingDate, propertyData));
            }
        }

        PropertyMessage[] propertyDataArray = data.toArray(new PropertyMessage[0]);
        return new BatchMessage(uuid, partitionId, timestamp, query, propertyDataArray);
    }


    /**
     * Get the date of the latest update to the database
     */
    private String getLatestDatabaseEntry(String tableName, Query query) {
        return databaseWrapper.getLastWriteDate(tableName, query.getCounty());
    }

    /**
     * Check data sources for new data and pull it into
     */
    private PropertyMessage[] pullNewDataFromSource(String tableName, String latestEntryDate) {
        return mockDataSource.getPropertyListings(tableName, latestEntryDate, LocalDate.now().toString());
    }

    /**
     * Update database with new data
     */
    private void updateDatabase(String tableName) {

    }


    /**
     * Send data to be processed
     * @param message
     */
    public void sendData(final String topic, final BatchMessage message) throws JsonProcessingException {
        TestCallback callback = new TestCallback();
        queryPublisher.send(new ProducerRecord<>(topic, Util.objectMapper.writeValueAsString(message)), callback);
    }

    public void testSendData(final String topic, final PropertyMessage[] message) throws JsonProcessingException {
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


    private PropertyData propertyItemToPropertyData(Item propertyItem) {
        String county = propertyItem.getString("County");
        double price = propertyItem.getNumber("Price").doubleValue();

        String propertyType = "house";
        if (propertyItem.isPresent("ListingType")) {
            propertyType = propertyItem.getString("ListingType");
        }

        String postcode = "000 0000";
        if (propertyItem.isPresent("Postcode")) {
            postcode = propertyItem.getString("Postcode");
        }

        Map<String, Object> additionalPropertyData = propertyItem.asMap();
        additionalPropertyData.remove("County");
        additionalPropertyData.remove("Price");
        additionalPropertyData.remove("ListingType");
        additionalPropertyData.remove("Postcode");

        return new PropertyData(county, propertyType, price, postcode, additionalPropertyData);
    }

    private KafkaProducer initQueryPublisher() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "0.0.0.0:9093");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("max.message.bytes", "100000");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer(props);
    }
}
