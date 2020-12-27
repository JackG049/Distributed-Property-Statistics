package puller;

import message.PropertyMessage;
import message.RequestMessage;
import model.Query;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class MockPullerTest {
    private MockPuller mockPuller = new MockPuller();

    @Test
    public void sendingTest() {
        Query query = new Query("Galway", "house", "XXX", LocalDate.now().toString()
                , LocalDate.now().toString(), 1000.0, 1500.0);
        RequestMessage message = new RequestMessage(UUID.randomUUID(), 0, query, LocalTime.now().toSecondOfDay());
        mockPuller.query(message);
    }

}
