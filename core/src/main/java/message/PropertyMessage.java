package message;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import model.PropertyData;

import java.time.LocalDate;

/**
 * {@link Message} implementation responsible for storing data relating to a property
 */
@Getter  @EqualsAndHashCode
public class PropertyMessage implements Message<PropertyData> {
    private final long timestamp;
    private final LocalDate localDate;
    private final PropertyData data;

    public PropertyMessage(@JsonProperty("timestamp") final long timestamp,
                           @JsonProperty("date") final LocalDate localDate,
                           @JsonProperty("data") final PropertyData propertyData) {
        this.timestamp = timestamp;
        this.localDate = localDate;
        this.data = Preconditions.checkNotNull(propertyData, "propertyData must not be null");
    }

    @JsonIgnore
    public double getPropertyPrice() {
        return data.getPrice();
    }
}
