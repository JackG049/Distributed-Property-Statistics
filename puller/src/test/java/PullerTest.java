import com.amazonaws.SdkClientException;
import com.amazonaws.services.dynamodbv2.document.*;
import message.BatchMessage;
import model.Query;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import puller.PropertyDbWrapper;
import puller.Puller;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class PullerTest {
    private static Puller puller;
    private static String DEFAULT_TABLE_NAME = "daft_ie";
    private static PropertyDbWrapper databaseWrapper = new PropertyDbWrapper();

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

    @AfterClass
    public static void tearDown() {
        databaseWrapper.deleteTable(DEFAULT_TABLE_NAME);
    }

    @Test
    public void pullFromDatabaseTest()  {
        Query query = new Query("Galway", null, null,
                "2020-12-01", "2020-12-25", null, null);

        ItemCollection<QueryOutcome> items = puller.queryDatabase(DEFAULT_TABLE_NAME, query);

        Iterator<Item> iterator = items.iterator();
        Item item = null;
        while (iterator.hasNext()) {
            item = iterator.next();
            System.out.println(item.toJSONPretty());
        }

    }


    @Test
    public void packageData()  {
        Query query = new Query("Galway", null, null,
                "2020-12-01", "2020-12-25", null, null);

        List<ItemCollection<QueryOutcome>> items = new ArrayList<>();
        items.add(puller.queryDatabase(DEFAULT_TABLE_NAME, query));
        BatchMessage message = puller.packageData(query, items);

        System.out.println(message.getData()[0].getLocalDate().toString());

    }

}
