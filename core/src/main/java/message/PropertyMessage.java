package message;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Map;

/**
 * {@link Message} implementation responsible for storing data relating to a property
 */
@Getter @EqualsAndHashCode
public class PropertyMessage implements Message<Map<String, Object>>{
    private final long timestamp;
    private final Map<String, Object> data;

    public PropertyMessage(@JsonProperty("timestamp") long timestamp,
                           @JsonProperty("data") final Map<String, Object> data) {
        this.timestamp = timestamp;
        this.data = Preconditions.checkNotNull(data, "propertyData must not be null");
    }

}
