package puller;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.collect.Sets;
import message.PropertyMessage;
import model.Query;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static util.DynamoDbUtil.propertyMessageToPropertyItem;

/**
 * The eponymous puller class asynchronously pulls property data from a data source and stores it in a database. It
 * can perform basic queries on this database to return property data.
 */

public class Puller {
    private static final PropertyDbWrapper databaseWrapper = new PropertyDbWrapper();
    private static final MockDataSource mockDataSource = new MockDataSource();
    private static final long PULL_NEW_LISTINGS_PERIOD_SECONDS = 5L;
    private final Set<String> DEFAULT_TABLES = Set.of("daft", "myhome");

    private static final Set<String> tables = new HashSet<>();
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public Puller() {
        // Create the default tables if they don't already exist
        tables.addAll(databaseWrapper.getTableNames());
        Set<String> absentTables = Sets.difference(DEFAULT_TABLES, tables);

        for (String tableName : absentTables) {
            try {
                databaseWrapper.createPropertyTable(tableName);
                tables.add(tableName);
            } catch (InterruptedException e) {
                System.out.println("Failed to create table " + tableName);
                e.printStackTrace();
            }
        }

        // Pull new property listings from a data source
        scheduler.scheduleAtFixedRate(Puller::pullNewListings, 0, PULL_NEW_LISTINGS_PERIOD_SECONDS, TimeUnit.SECONDS);
    }


    /**
     * Query all the property tables for relevant property data
     * @param query to be performed
     * @return property data for each data source table e.g daft.ie and myhomes.ie map to lists of prop data
     */
    public static Map<String, List<PropertyMessage>> getQueryData(Query query) {
        Map<String, List<PropertyMessage>> queryData = new HashMap<>();

        for (String tableName : tables) {
            queryData.put(tableName, queryDatabase(tableName, query));
        }

        return queryData;
    }


    /**
     * Query a property tables for relevant property data
     * @param query to be performed
     * @return relevant property data from the provided table
     */
    private static List<PropertyMessage> queryDatabase(String tableName, Query query) {
        List<PropertyMessage> result;
        result = databaseWrapper.queryTable(tableName, query);
        return result;
    }

    /**
     * For each table (daft.ie, myhomes) check for new listings and update the database
     */
    private static void pullNewListings() {
        for (String tableName : tables) {
            String latestEntryDate = getLatestDatabaseEntry(tableName);
            Map<String, PropertyMessage> newPropertyListings = pullNewDataFromSource(tableName, latestEntryDate);
            if (!newPropertyListings.isEmpty()) {
                storeListings(tableName, newPropertyListings);
            }
        }
    }

    /**
     * Find when the latest property has been added
     * @param tableName to be searched
     * @return the date of the latest write in YYYY-MM-DD format
     */
    private static String getLatestDatabaseEntry(String tableName) {
        return databaseWrapper.getLastWriteDate(tableName);
    }

    /**
     * Check data sources for new data / new property listings
     * @return new property listings
     */
    private static Map<String, PropertyMessage> pullNewDataFromSource(String tableName, String latestEntryDate) {
        return mockDataSource.getPropertyListings(tableName, latestEntryDate, LocalDate.now().toString());
    }

    /**
     * Update database with new data / property listings
     */
    private static void storeListings(String tableName, Map<String, PropertyMessage> propertyMessages) {
        List<Item> propertyItems = new ArrayList<>(propertyMessages.size());
        propertyMessages.forEach((k,v) -> {
            Item item = propertyMessageToPropertyItem(v);
            item.withString("ListingId", k);
            propertyItems.add(item);
        });

        databaseWrapper.batchWriteItem(tableName, propertyItems);
    }

}
