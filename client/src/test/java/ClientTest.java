import static org.junit.Assert.assertNotNull;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import message.MessageDeserializer;
import results.ResultsHandler;
import util.Util;

public class ClientTest {

    private Properties props;
    private MessageDeserializer deserializer;

    @Before
    public void init() {
        props = Util.loadPropertiesFromFile("consumertest.properties");
        props.setProperty("group.id",  "results_client");
        deserializer = new MessageDeserializer(Util.objectMapper);
    }

    @Test
    public void testKafkaConnection() {
        ResultsHandler resultsHandler = new ResultsHandler(props, deserializer);
        assertNotNull(resultsHandler);
    }
}
