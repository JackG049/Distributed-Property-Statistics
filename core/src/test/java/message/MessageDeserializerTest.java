package message;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.PropertyData;
import model.Query;
import org.junit.Before;
import org.junit.Test;
import utils.TestUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class MessageDeserializerTest {

    private final ObjectMapper objectMapper = TestUtils.objectMapper;
    private MessageDeserializer deserializer;

    @Before
    public void setup() {
        deserializer = new MessageDeserializer(objectMapper);
    }

    @Test
    public void canDeserializePropertyMessage() throws IOException {
        final PropertyData propertyData = new PropertyData("county","xyz", 10001, "D01DF11");

        final Message<?> propertyMessage = new PropertyMessage(Instant.EPOCH.toEpochMilli(), LocalDate.now(), propertyData);
        String data = objectMapper.writeValueAsString(propertyMessage);
        final Message<?> deserializedMessage = deserializer.deserialize(data);
        System.out.println(objectMapper.writeValueAsString(deserializedMessage));
        assertNotNull(deserializedMessage);
        assertEquals(propertyMessage, deserializedMessage);
    }

    @Test
    public void canDeserializeBatchMessage() throws IOException {
        final PropertyMessage[] messages = new PropertyMessage[10];
        for (int i = 0; i < 10; i++) {
            messages[i] = new PropertyMessage(Instant.now().toEpochMilli() + i, LocalDate.now(), new PropertyData("mayo", "prop" + i, 10, "D" + i));
        }
        final Message<PropertyMessage[]> batchMessage = new BatchMessage(UUID.randomUUID(),1, Instant.EPOCH.toEpochMilli(), new Query("mayo", null, null, null, null, null, null), messages);
        final String data = objectMapper.writeValueAsString(batchMessage);
        final Message<?> deserializedMessage = deserializer.deserialize(data);
        assertNotNull(deserializedMessage);
        assertEquals(batchMessage, deserializedMessage);
    }
}
