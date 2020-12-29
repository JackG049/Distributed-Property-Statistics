package puller;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import message.PropertyMessage;
import model.Query;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PropertyDbWrapperTest {
    private final static PropertyDbWrapper databaseWrapper = new PropertyDbWrapper();
    private static String DEFAULT_TABLE_NAME = "daft";
    private static Set<String> tables = Set.of(DEFAULT_TABLE_NAME);

    @BeforeClass
    public static void setup() throws InterruptedException {
        databaseWrapper.createPropertyTable(DEFAULT_TABLE_NAME);

        for (int i = 0; i < 4; i++) {
            final Map<String, Object> infoMap = new HashMap<String, Object>();
            infoMap.put("Price", Math.random() * 1500);
            infoMap.put("County", "Galway");

            databaseWrapper.writeData(DEFAULT_TABLE_NAME, "Daft_" + i, "2020-12-0" + (i + 1), infoMap);
        }

        final Map<String, Object> infoMap = new HashMap<String, Object>();
        infoMap.put("Price", 2000);
        infoMap.put("County", "Galway");
        infoMap.put("PropertyType", "house");
        databaseWrapper.writeData(DEFAULT_TABLE_NAME, "Daft_" + 4, "2020-12-0" + 5, infoMap);
    }

    @AfterClass
    public static void tearDown() {
        databaseWrapper.deleteTable(DEFAULT_TABLE_NAME);
    }

    @Test
    public void queryTableTest()  {
        List<PropertyMessage> items = databaseWrapper.queryTable(DEFAULT_TABLE_NAME, "2020-12-01", "2020-12-24", "Galway");
        assertTrue(!items.isEmpty());
    }

    @Test
    public void priceQueryTableTest() {
        Query query = new Query("Galway", "house", "000",
                "2020-12-01", "2020-12-25", 2000.0, 2000.0);
        List<PropertyMessage> items = databaseWrapper.queryTable(DEFAULT_TABLE_NAME, query);
        assertEquals(1, items.size());

        Query queryApartment = new Query("Galway", "apartment", "000",
                "2020-12-01", "2020-12-25", 2000.0, 2000.0);
        List<PropertyMessage> apartmentItems = databaseWrapper.queryTable(DEFAULT_TABLE_NAME, queryApartment);
        assertEquals(0, apartmentItems.size());
    }

    @Test
    public void batchWriteTest() {
        Map<String, Object> propertyData = ImmutableMap.of("Price", 500, "County", "OldMen");
        List<Item> items = Lists.newArrayList(
                databaseWrapper.buildPropertyItem("test_1", "2020-12-01", propertyData),
                databaseWrapper.buildPropertyItem("test_2", "2020-12-01", propertyData));

        databaseWrapper.batchWriteItem(DEFAULT_TABLE_NAME, items);

        List<PropertyMessage> result = databaseWrapper.queryTable(DEFAULT_TABLE_NAME, "2020-12-01", "2020-12-01", "OldMen");
        assertEquals(2, result.size());
    }

    @Test
    public void getLastWriteTest() {
        String date = databaseWrapper.getLastWriteDate(DEFAULT_TABLE_NAME);
        assertEquals( "2020-12-05", date);
    }


}
