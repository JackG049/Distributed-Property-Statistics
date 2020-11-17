package message;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

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
