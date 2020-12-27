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

import java.time.LocalDate;
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

        ItemCollection<QueryOutcome> items = puller.queryDatabase(DEFAULT_TABLE_NAME, query);

        Iterator<Item> iterator = items.iterator();
        Item item = null;
        while (iterator.hasNext()) {
            item = iterator.next();
            //System.out.println(item.toJSONPretty());
        }

    }


    @Ignore
    @Test
    public void packageData()  {
        Query query = new Query("Galway", null, null,
                "2020-12-01", "2020-12-25", null, null);

        List<ItemCollection<QueryOutcome>> items = new ArrayList<>();
        items.add(puller.queryDatabase(DEFAULT_TABLE_NAME, query));
        BatchMessage message = puller.packageData(query, items);

        //System.out.println(message.getData()[0].getLocalDate().toString());

    }

    @Ignore
    @Test
    public void publishTest() throws JsonProcessingException {
        /*
        PropertyMessage[] messages = new PropertyMessage[1];
        messages[0] = new PropertyMessage(
                System.currentTimeMillis(), LocalDate.now(), new PropertyData("Galway", "house", 1000.0, "D18", ImmutableMap.of())
        );

        final BatchMessage batchMessage = new BatchMessage(UUID.randomUUID(), 0, System.currentTimeMillis(),
                new Query("Galway", "house", null, null, null, null, null), messages);

       puller.sendData(KafkaConstants.REQUESTS_DAFT, batchMessage);
         */

    }

}
