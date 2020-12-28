package puller;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import counties.County;
import message.PropertyMessage;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

import static util.DynamoDbUtil.propertyItemToPropertyMessage;

public class PropertyDbWrapper {
    private AmazonDynamoDB client;
    private DynamoDB dynamoDB;
    private final String DEFAULT_ORIGIN_DATE = "2020-01-01";

    public PropertyDbWrapper() {
        this.client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://127.0.0.1:8000", "eu-west-2"))
                .build();
        this.dynamoDB = new DynamoDB(client);
    }

    public Set<String> getTableNames() {
        TableCollection<ListTablesResult> tables = dynamoDB.listTables();
        Set<Table> tableSet = Sets.newHashSet(tables.iterator());
        return tableSet.stream().map(Table::getTableName).collect(Collectors.toSet());
    }

    public String getLastWriteDate(String tableName) {
        Table table = dynamoDB.getTable(tableName);
        Index index = table.getIndex("ListingDateIndex");
        LocalDate latestWriteDate = LocalDate.parse(DEFAULT_ORIGIN_DATE);

        for (String county : County.getCounties()) {
            QuerySpec request = new QuerySpec()
                    .withScanIndexForward(false)
                    .withMaxResultSize(1)
                    .withKeyConditionExpression("#pk = :county")
                    .withNameMap(new NameMap().with("#pk", "County"))
                    .withValueMap(new ValueMap()
                            .withString(":county", county));

            ItemCollection<QueryOutcome> item = index.query(request);
            IteratorSupport<Item, QueryOutcome> itemIterator = item.iterator();

            if (itemIterator.hasNext()) {
                LocalDate partitionDate = LocalDate.parse(itemIterator.next().getString("ListingDate"));
                if (partitionDate.isAfter(latestWriteDate)) {
                    latestWriteDate = partitionDate;
                }
            }
        }

        return latestWriteDate.toString();
    }

    public List<PropertyMessage> queryTable(String tableName, String periodStart, String periodEnd, String county) {
        Table table = dynamoDB.getTable(tableName);
        Index index = table.getIndex("ListingDateIndex");

        QuerySpec request = new QuerySpec()
                .withKeyConditionExpression("#pk = :county and #sk between :start and :end")
                .withNameMap(new NameMap().with("#pk", "County").with("#sk", "ListingDate"))
                .withValueMap(new ValueMap()
                        .withString(":county", county)
                        .withString(":start", periodStart)
                        .withString(":end", periodEnd));

        ItemCollection<QueryOutcome> items = index.query(request);

        List<PropertyMessage> propertyMessages = new ArrayList<>();
        IteratorSupport<Item, QueryOutcome> dataIterator = items.iterator();
        while (dataIterator.hasNext()) {
            propertyMessages.add(
                    propertyItemToPropertyMessage(dataIterator.next())
            );
        }
        return propertyMessages;
    }

    public void writeData(String tableName, String listingId, String listingDate, Map<String, Object> additionalData) {
        Table table = dynamoDB.getTable(tableName);

        try {
            System.out.println("Adding a new item...");
            Item item = buildPropertyItem(listingId, listingDate, additionalData);
            PutItemOutcome outcome = table.putItem(item);
            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());

        }
        catch (Exception e) {
            System.err.println("Unable to add item: " + listingId + " " + listingDate);
            System.err.println(e.getMessage());
        }
    }

    public void writePropertyItem(String tableName, Item propertyItem) {
        Table table = dynamoDB.getTable(tableName);

        try {
            System.out.println("Adding a new item...");
            PutItemOutcome outcome = table.putItem(propertyItem);
            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());
        }
        catch (Exception e) {
            System.err.println("Unable to add item");
            System.err.println(e.getMessage());
        }
    }


    public void batchWriteItem(String table, List<Item> items) {
        TableWriteItems batchWrites = new TableWriteItems(table).withItemsToPut(items);
        BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(batchWrites);

        do {
            // Check for unprocessed keys which could happen if you exceed
            // provisioned throughput
            Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();
            if (!outcome.getUnprocessedItems().isEmpty()) {
                outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
            }

        } while (outcome.getUnprocessedItems().size() > 0);

    }

    public void deleteTable(String tableName) {
        System.out.println("Deleting table " + tableName + "...");
        Table table = dynamoDB.getTable(tableName);
        table.delete();

        System.out.println("Waiting for " + tableName + " to be deleted...");
        try {
            table.waitForDelete();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Item buildPropertyItem(String listingId, String listingDate, Map<String, Object> additionalData) {
        Item item = new Item().withPrimaryKey("ListingId", listingId, "ListingDate", listingDate);
        for (Map.Entry<String, Object> entry : additionalData.entrySet()) {
            if (entry.getValue() instanceof String) {
                item.withString(entry.getKey(), (String) entry.getValue());
            } else if (entry.getValue() instanceof Number) {
                item.withNumber(entry.getKey(), (Number) entry.getValue());
            }
        }

        return item;
    }


    public void createPropertyTable(String tableName) throws InterruptedException {
        System.out.println("Attempting to create table; please wait...");

        // Attribute definitions
        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();

        attributeDefinitions.add(new AttributeDefinition().withAttributeName("ListingId").withAttributeType(ScalarAttributeType.S));
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("ListingDate").withAttributeType(ScalarAttributeType.S));
        //attributeDefinitions.add(new AttributeDefinition().withAttributeName("Price").withAttributeType(ScalarAttributeType.N));
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("County").withAttributeType(ScalarAttributeType.S));

        // Key schema for table
        ArrayList<KeySchemaElement> tableKeySchema = new ArrayList<KeySchemaElement>();
        tableKeySchema.add(new KeySchemaElement().withAttributeName("ListingId").withKeyType(KeyType.HASH)); // Partition
        tableKeySchema.add(new KeySchemaElement().withAttributeName("ListingDate").withKeyType(KeyType.RANGE));

        // Initial provisioned throughput settings for the indexes
        ProvisionedThroughput ptIndex = new ProvisionedThroughput().withReadCapacityUnits(1L)
                .withWriteCapacityUnits(1L);

        // CreateDateIndex
        GlobalSecondaryIndex listingDateIndex = new GlobalSecondaryIndex().withIndexName("ListingDateIndex")
                .withProvisionedThroughput(ptIndex)
                .withKeySchema(new KeySchemaElement().withAttributeName("County").withKeyType(KeyType.HASH), // Partition
                        new KeySchemaElement().withAttributeName("ListingDate").withKeyType(KeyType.RANGE)) // Sort
                .withProjection(new Projection().withProjectionType("ALL"));

        CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                .withProvisionedThroughput(
                        new ProvisionedThroughput().withReadCapacityUnits((long) 1).withWriteCapacityUnits((long) 1))
                .withAttributeDefinitions(attributeDefinitions).withKeySchema(tableKeySchema)
                .withGlobalSecondaryIndexes(listingDateIndex);


        System.out.println("Creating table " + tableName + "...");
        dynamoDB.createTable(createTableRequest);

        // Wait for table to become active
        System.out.println("Waiting for " + tableName + " to become ACTIVE...");
        try {
            Table table = dynamoDB.getTable(tableName);
            table.waitForActive();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}