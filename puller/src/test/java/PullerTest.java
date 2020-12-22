import com.amazonaws.SdkClientException;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class PullerTest {
    private Puller puller = new Puller();
    private static String DEFAULT_TABLE_NAME = "daft_ie";

    @BeforeClass
    public static void setup() {

    }

    @Test
    public void pullFromDatabaseTest()  {
        /*
        ItemCollection<QueryOutcome> items = puller.pullFromDatabase(DEFAULT_TABLE_NAME, "2020-12-01", "2020-12-24", new HashMap<>());

        Iterator<Item> iterator = items.iterator();
        Item item = null;
        while (iterator.hasNext()) {
            item = iterator.next();
            System.out.println(item.toJSONPretty());
        }

         */
    }


}
