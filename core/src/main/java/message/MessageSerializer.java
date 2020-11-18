package message;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Really simple message serializer, all of the type information is handled through Jackson and as long as the class to
 * be serialized is set up properly then this will result in byte array that is deserializable
 */
public class MessageSerializer {
    private final ObjectMapper objectMapper;

    public MessageSerializer(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public byte[] serialize(final Message<?> message){
        try {
            return objectMapper.writeValueAsBytes(message);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
