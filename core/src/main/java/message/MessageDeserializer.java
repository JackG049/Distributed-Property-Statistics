package message;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Simple deserializer using Jackson, provided all is setup correctly in the class being deserialized then this should have
 * no problem deserializing to the class from a byte[].
 */
public class MessageDeserializer {
    private final ObjectMapper objectMapper;

    public MessageDeserializer(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public <S> Message<S> deserialize(final String dataAsString){
        try {
            return objectMapper.readValue(dataAsString, Message.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
