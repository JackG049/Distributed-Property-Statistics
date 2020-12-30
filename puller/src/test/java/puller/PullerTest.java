package puller;


import message.PropertyMessage;
import model.Query;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import java.util.*;

@Ignore
public class PullerTest {
    private static Puller puller;
    private static String DEFAULT_TABLE_NAME = "daft";
    private static PropertyDbWrapper databaseWrapper = new PropertyDbWrapper("http://localhost:8000");
    private static final long DEFAULT_NUM_ENTRIES = 5;

    @BeforeClass
    public static void setup() {
        puller = new Puller("http://localhost:8000");

        for (int i = 0; i < DEFAULT_NUM_ENTRIES; i++) {
            final Map<String, Object> infoMap = new HashMap<String, Object>();
            infoMap.put("Price", Math.random() * 1500);
            infoMap.put("County", "Galway");
            infoMap.put("PropertyType", "house");

            databaseWrapper.writeData(DEFAULT_TABLE_NAME, "daft_" + i, "2020-12-0" + (i + 1), infoMap);
        }
    }

    @AfterClass
    public static void tearDown() {
        databaseWrapper.deleteTable(DEFAULT_TABLE_NAME);
    }

    @Test
    public void getQueryDataTest()  {
        Query query = new Query("Galway", "house", "000",
                "2020-12-01", "2020-12-25", 500.0, 2000.0);

        Map<String, List<PropertyMessage>> result = puller.getQueryData(query);
        assertTrue(!result.isEmpty());
    }

    @Test
    public void databasePopulationTest() throws InterruptedException {
        Thread.sleep(5000);
        long size = databaseWrapper.getApproxTableSize("daft");
        assertTrue(size > DEFAULT_NUM_ENTRIES);
    }
}
