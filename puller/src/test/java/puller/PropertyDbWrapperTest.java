package puller;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class PropertyDbWrapperTest {
    private final static PropertyDbWrapper databaseWrapper = new PropertyDbWrapper();
    private static boolean isDatabaseRunning = false;
    private static Set<String> tables = new HashSet();
    private static String DEFAULT_TABLE_NAME = "daft_ie";
    private static boolean hitIf = false;

    @Ignore
    @BeforeClass
    public static void setup() {

        try {
            if (databaseWrapper.getTableNames().isEmpty()) {
                System.out.println(tables);
                databaseWrapper.createPropertyTable(DEFAULT_TABLE_NAME);

            }
            tables.addAll(databaseWrapper.getTableNames());

            for (String table : tables) {
                System.out.println("Table " + table);
            }
            isDatabaseRunning = true;


            int propertyId = 0;

            while (propertyId < 5) {
                final Map<String, Object> infoMap = new HashMap<String, Object>();
                infoMap.put("Price", Math.random()*1500);
                infoMap.put("County", "Galway");

                databaseWrapper.writeData(DEFAULT_TABLE_NAME, "Daft_" + propertyId, "2020-12-0" + (propertyId+1), infoMap);
                propertyId++;
            }


        } catch (SdkClientException e) {
            System.out.println("The database is not running. Aborting further tests. " + e);
        } catch (InterruptedException e) {
            System.out.println("Initial table creation interupted. Aborting further tests. " + e);
        }
    }

    @Ignore
    @AfterClass
    public static void tearDown() {
        databaseWrapper.deleteTable(DEFAULT_TABLE_NAME);
    }

    @Ignore
    @Test
    public void getTableNamesTest()  {
        assumeTrue(isDatabaseRunning);
        Set<String> tableNames = databaseWrapper.getTableNames();
        assertEquals(tableNames, tables);
    }

    @Ignore
    @Test
    public void queryTableTest()  {
        assumeTrue(isDatabaseRunning);
        ItemCollection<QueryOutcome> items = databaseWrapper.queryTable(DEFAULT_TABLE_NAME, "2020-12-01", "2020-12-24", "Galway");

        Iterator<Item> iterator = items.iterator();
        Item item = null;
        while (iterator.hasNext()) {
            item = iterator.next();
            System.out.println(item.toJSONPretty());
        }

    }

    @Ignore
    @Test
    public void batchWriteTest() {
        Map<String, Object> propertyData = ImmutableMap.of("Price", 500, "County", "Galway");
        List<Item> items = Lists.newArrayList(
                databaseWrapper.buildPropertyItem("Daft_2", "2020-12-06", propertyData),
                databaseWrapper.buildPropertyItem("Daft_3", "2020-12-06", propertyData));

        databaseWrapper.batchWriteItem(DEFAULT_TABLE_NAME, items);
    }


    @Ignore
    @Test
    public void getLastWriteTest() {
        String date = databaseWrapper.getLastWriteDate(DEFAULT_TABLE_NAME, "Galway");
        System.out.println("Date: " + date);
    }

    @Ignore
    @Test
    public void loadPropertyData() {
        //databaseWrapper.loadPropertyData(DEFAULT_TABLE_NAME, "");
    }


}
