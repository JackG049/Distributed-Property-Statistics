import com.amazonaws.SdkClientException;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class PropertyDbWrapperTest {
    private final static PropertyDbWrapper databaseWrapper = new PropertyDbWrapper();
    private static boolean isDatabaseRunning = false;
    private static Set<String> tables = new HashSet();
    private static String DEFAULT_TABLE_NAME = "daft_ie";

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


            final Map<String, Object> infoMap = new HashMap<String, Object>();
            infoMap.put("Price", 850);
            infoMap.put("County", "Galway");

            //databaseWrapper.writeData(DEFAULT_TABLE_NAME, "Daft_1", "2020-12-01", infoMap);
            databaseWrapper.writeData(DEFAULT_TABLE_NAME, "Daft_1", "2020-12-01", infoMap);


        } catch (SdkClientException e) {
            System.out.println("The database is not running. Aborting further tests. " + e);
        } catch (InterruptedException e) {
            System.out.println("Initial table creation interupted. Aborting further tests. " + e);
        }
    }

    @AfterClass
    public static void tearDown() {
        databaseWrapper.deleteTable(DEFAULT_TABLE_NAME);
    }

    @Test
    public void getTableNamesTest()  {
        assumeTrue(isDatabaseRunning);
        Set<String> tableNames = databaseWrapper.getTableNames();
        assertEquals(tableNames, tables);
    }

    @Test
    public void queryTableTest()  {
        assumeTrue(isDatabaseRunning);
        ItemCollection<QueryOutcome> items = databaseWrapper.queryTable("daft_ie", "2020-12-01", "2020-12-24", "Galway");

        Iterator<Item> iterator = items.iterator();
        Item item = null;
        while (iterator.hasNext()) {
            item = iterator.next();
            System.out.println(item.toJSONPretty());
        }

    }

    @Test
    public void batchWriteTest() {
        Map<String, Object> propertyData = ImmutableMap.of("Price", 500, "County", "Galway");
        List<Item> items = Lists.newArrayList(
                databaseWrapper.buildPropertyItem("Daft_2", "2020-12-06", propertyData),
                databaseWrapper.buildPropertyItem("Daft_3", "2020-12-06", propertyData));

        databaseWrapper.batchWriteItem(DEFAULT_TABLE_NAME, items);
    }

    @Test
    public void loadPropertyData() {
        //databaseWrapper.loadPropertyData(DEFAULT_TABLE_NAME, "");
    }


}
