package puller;

import com.amazonaws.services.dynamodbv2.document.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import kafka.KafkaConstants;
import message.BatchMessage;
import message.PropertyMessage;
import model.PropertyData;
import model.Query;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import java.util.*;


public class PullerTest {
    private static Puller puller;
    private static String DEFAULT_TABLE_NAME = "daft_ie";
    private static PropertyDbWrapper databaseWrapper = new PropertyDbWrapper();

    @Ignore
    @BeforeClass
    public static void setup() {
        puller = new Puller();

        int propertyId = 0;

        while (propertyId < 5) {
            final Map<String, Object> infoMap = new HashMap<String, Object>();
            infoMap.put("Price", Math.random() * 1500);
            infoMap.put("County", "Galway");

            databaseWrapper.writeData(DEFAULT_TABLE_NAME, "Daft_" + propertyId, "2020-12-0" + propertyId, infoMap);
            propertyId++;
        }
    }

    @Ignore
    @AfterClass
    public static void tearDown() {
        databaseWrapper.deleteTable(DEFAULT_TABLE_NAME);
    }


    @Ignore
    @Test
    public void pullFromDatabaseTest()  {
        Query query = new Query("Galway", null, null,
                "2020-12-01", "2020-12-25", null, null);

        List<PropertyMessage> items = puller.queryDatabase(DEFAULT_TABLE_NAME, query);
        assertTrue(!items.isEmpty());
    }



}
