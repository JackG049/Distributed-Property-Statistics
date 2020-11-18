package message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Essentially a wrapper class around {@link PropertyMessage}s. Enables us to collect all information for properties from the
 * Database, create a single {@link PropertyMessage} for each of them and then batch them together for sending.
 *
 * This is necessary since we cannot process {@link PropertyMessage}s individually because we need to know how many datapoints
 * are to be processed so we know when to publish results.
 */
@Getter @EqualsAndHashCode
public class BatchMessage implements Message<PropertyMessage[]> {
    private final long batchId;
    private final long timestamp;
    private final PropertyMessage[] data;

    @JsonCreator
    public BatchMessage(@JsonProperty("batchId") final long batchId,
                        @JsonProperty("timestamp") final long timestamp,
                        @JsonProperty("data") final PropertyMessage[] data) {
        this.batchId = batchId;
        Preconditions.checkArgument(timestamp >= 0, "timestamp must be greater than zero");
        this.timestamp = timestamp;
        this.data = Preconditions.checkNotNull(data, "messages must not be null");
    }

}
