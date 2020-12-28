package puller;

import message.RequestMessage;
import model.Query;
import org.junit.Before;
import org.junit.Test;
import util.Util;
import static org.junit.Assert.*;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Properties;
import java.util.UUID;


public class QueryHandlerTest {
    private QueryHandler queryHandler;
    private Properties props;

    @Before
    public void init() {
        props = Util.loadPropertiesFromFile("producer.properties");
        queryHandler = new QueryHandler();
    }

    @Test
    public void queryPullerTest() {
        Query query = new Query("Galway", "house", "XXX", LocalDate.now().toString()
                , LocalDate.now().toString(), 1000.0, 1500.0);
        RequestMessage message = new RequestMessage(UUID.randomUUID(), 0, query, LocalTime.now().toSecondOfDay());
        assertNotNull(queryHandler);
        queryHandler.query(message);
    }

}

