package utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import message.BatchMessage;
import message.PropertyMessage;
import model.Query;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.time.LocalDate;

public class TestUtils {

    public static final ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
        objectMapper
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true)
                .registerSubtypes(getSubtypes());
    }

    private static NamedType[] getSubtypes() {
        return new NamedType[] {
                new NamedType(BatchMessage.class, "batch"),
                new NamedType(PropertyMessage.class, "property")
        };
    }

    public static Query emptyQuery() {
        return new Query(null, null, null,null, null, null, null);
    }

}
