import static org.junit.Assert.assertNotNull;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import message.MessageDeserializer;
import results.ResultsHandler;
import util.Util;

public class ResultsHandlerTest {
    private ResultsHandler resultsHandler;
    private Properties props;
    private MessageDeserializer deserializer;

    @Before
    public void init() {
        deserializer = new MessageDeserializer(Util.objectMapper);
        props = Util.loadPropertiesFromFile("consumertest.properties");
        resultsHandler = new ResultsHandler(props,deserializer);
    }

    @Test
    public void checkNotNull() {
        assertNotNull(resultsHandler);
    } 
}
