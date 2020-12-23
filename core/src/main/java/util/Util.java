package util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import message.BatchMessage;
import message.PropertyMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Util {
    private static final Logger LOGGER = LoggerFactory.getLogger(Util.class);

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

    public static Properties loadPropertiesFromFile(final String fileName) {
        final Properties properties = new Properties();

        try (final InputStream propertiesInputStream = Util.class.getClassLoader().getResourceAsStream(fileName)) {
            LOGGER.info("Attempting to load properties from " + fileName + "...");
            properties.load(propertiesInputStream);
            LOGGER.info("Success");

        } catch (final IOException ex) {
            LOGGER.warn("Failure");
            ex.printStackTrace();
            /*
                Do we fail fully and fast or do we try to recover?
             */
        }
        return properties;
    }

}
