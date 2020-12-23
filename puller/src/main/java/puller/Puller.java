package puller;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.google.common.collect.Sets;
import message.BatchMessage;
import message.PropertyMessage;
import model.PropertyData;
import model.Query;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

@RestController
public class Puller {
    private final PropertyDbWrapper databaseWrapper = new PropertyDbWrapper();
    private QueryPublisher queryPublisher;
    private Set<String> tableNames;

    public Puller() {
        //queryPublisher = new QueryPublisher();
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
     *
     * @param query
     */
    @RequestMapping(value = "/query", method = RequestMethod.POST)
    public void query(@RequestBody Query query) { //request message todo
        List<ItemCollection<QueryOutcome>> rawQueryData = new ArrayList<>();
        for (String tableName : tableNames) {
            getLatestDatabaseEntry(tableName, query);
            //pullNewDataFromSource(tableName);
            //updateDatabase(tableName);
            rawQueryData.add(queryDatabase(tableName, query));
        }

        BatchMessage message = packageData(query, rawQueryData);
        sendData(message);
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
     * Send data to be processed
     *
     * @param message
     */
    private void sendData(BatchMessage message) {
        queryPublisher.publish(message);
    }

    /**
     * Get the date of the latest update to the database
     */
    private String getLatestDatabaseEntry(String tableName, Query query) {
        return databaseWrapper.getLastWriteDate(tableName, query.getCounty());
    }

    /**
     * Check data sources for new data and pull it into
     *
     * @param tableName
     */
    private void pullNewDataFromSource(String tableName) {
        // pull from daft & myhomes

        // store data
    }

    /**
     * Update database with new data
     */
    private void updateDatabase(String tableName) {

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
}
