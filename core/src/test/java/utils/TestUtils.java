package utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import message.BatchMessage;
import message.PropertyMessage;
import model.Query;

public class TestUtils {

    public static final ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
        objectMapper
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                .registerSubtypes(getSubtypes());
    }

    private static NamedType[] getSubtypes() {
        return new NamedType[] {
                new NamedType(BatchMessage.class, "batch"),
                new NamedType(PropertyMessage.class, "property")
        };
    }

    public static Query emptyQuery() {
        return new Query(null, null, null, null, null);
    }

}
