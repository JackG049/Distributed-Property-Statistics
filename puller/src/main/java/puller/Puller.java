package puller;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.collect.Sets;
import message.PropertyMessage;
import model.Query;
import util.DynamoDbUtil;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static util.DynamoDbUtil.propertyMessageToPropertyItem;

public class Puller {
    private static final PropertyDbWrapper databaseWrapper = new PropertyDbWrapper();
    private static final MockDataSource mockDataSource = new MockDataSource();
    private static final long PULL_NEW_LISTINGS_PERIOD_SECONDS = 5L;
    private final Set<String> DEFAULT_TABLES = Set.of("daft", "myhome");

    private static final Set<String> tables = new HashSet<>();
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public Puller() {
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

        // Fetch client-quotation requests
        scheduler.scheduleAtFixedRate(Puller::pullNewListings, 5, PULL_NEW_LISTINGS_PERIOD_SECONDS, TimeUnit.SECONDS);
    }


    public static Map<String, List<PropertyMessage>> getQueryData(Query query) {
        Map<String, List<PropertyMessage>> queryData = new HashMap<>();

        for (String tableName : tables) {
            queryData.put(tableName, queryDatabase(tableName, query));
        }

        return queryData;
    }


    /**
     * Retrieve all relevant data from the database
     * @return
     */
    private static List<PropertyMessage> queryDatabase(String tableName, Query query) {
        String periodStart = query.getStartDate();
        String periodEnd = query.getEndDate();
        String county = query.getCounty();


        List<PropertyMessage> result;
        if (query.getMinPrice() != null && query.getMaxPrice() != null) {
            // todo add price range query
            result = databaseWrapper.queryTable(tableName, periodStart, periodEnd, county);
        } else {
            result = databaseWrapper.queryTable(tableName, periodStart, periodEnd, county);
        }

        return result;
    }

    private static void pullNewListings() {
        for (String tableName : tables) {
            String latestEntryDate = getLatestDatabaseEntry(tableName);
            Map<String, PropertyMessage> newPropertyListings = pullNewDataFromSource(tableName, latestEntryDate);
            storeListings(tableName, newPropertyListings);
        }
    }

    /**
     * Get the date of the latest update to the database
     */
    private static String getLatestDatabaseEntry(String tableName) {
        return databaseWrapper.getLastWriteDate(tableName);
    }

    /**
     * Check data sources for new data and pull it into
     * @return
     */
    private static Map<String, PropertyMessage> pullNewDataFromSource(String tableName, String latestEntryDate) {
        return mockDataSource.getPropertyListings(tableName, latestEntryDate, LocalDate.now().toString());
    }

    /**
     * Update database with new data
     */
    private static void storeListings(String tableName, Map<String, PropertyMessage> propertyMessages) {
        List<Item> propertyItems = new ArrayList<>(propertyMessages.size());
        propertyMessages.forEach((k,v) -> {
            Item item = propertyMessageToPropertyItem(v);
            item.withString("ListingId", k);
            propertyItems.add(item);
        });

        databaseWrapper.batchWriteItem(tableName, propertyItems);
        /*
        for (PropertyMessage propertyMessage : propertyMessages) {
            databaseWrapper.writePropertyItem(tableName, propertyMessageToPropertyItem(propertyMessage));
        }
         */
    }

    /*
          Set<String> databaseTables = databaseWrapper.getTableNames();
        Set<String> absentTables = Sets.difference(DEFAULT_TABLES, databaseTables);

        for (String absentTable : absentTables) {
            try {
                databaseWrapper.createPropertyTable(absentTable);
                tables.add(absentTable);
            } catch (InterruptedException e) {
                System.out.println("Failed to create table " + absentTable);
                e.printStackTrace();
            }
        }
     */


}
