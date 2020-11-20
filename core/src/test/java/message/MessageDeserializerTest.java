package message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import model.Query;
import org.junit.Before;
import org.junit.Test;
import utils.TestUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

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
        final Map<String, Object> propertyData = ImmutableMap.of("address", "xyz", "price", "10001");
        final Message<?> propertyMessage = new PropertyMessage(Instant.EPOCH.toEpochMilli(), propertyData);
        byte[] data = objectMapper.writeValueAsBytes(propertyMessage);
        final Message<?> deserializedMessage = deserializer.deserialize(data);
        assertNotNull(deserializedMessage);
        assertEquals(propertyMessage, deserializedMessage);
    }

    @Test
    public void canDeserializeBatchMessage() throws IOException {
        final PropertyMessage[] messages = new PropertyMessage[100];
        for (int i = 0; i < 100; i++) {
            messages[i] = new PropertyMessage(Instant.now().toEpochMilli() + i, ImmutableMap.of("testID", i));
        }
        final Message<PropertyMessage[]> batchMessage = new BatchMessage(1L, Instant.EPOCH.toEpochMilli(),TestUtils.emptyQuery(), messages);
        final byte[] data = objectMapper.writeValueAsBytes(batchMessage);
        final Message<?> deserializedMessage = deserializer.deserialize(data);
        assertNotNull(deserializedMessage);
        assertEquals(batchMessage, deserializedMessage);
    }
}
