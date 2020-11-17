package message;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class MessageDeserializer {
    private final ObjectMapper objectMapper;

    public MessageDeserializer(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public <S> Message<S> deserialize(final byte[] data){
        try {
            return objectMapper.readValue(data, Message.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
