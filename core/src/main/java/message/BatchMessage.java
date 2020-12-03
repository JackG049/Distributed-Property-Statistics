package message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import model.Query;

import java.util.UUID;

/**
 * Essentially a wrapper class around {@link PropertyMessage}s. Enables us to collect all information for properties from the
 * Database, create a single {@link PropertyMessage} for each of them and then batch them together for sending.
 *
 * This is necessary since we cannot process {@link PropertyMessage}s individually because we need to know how many datapoints
 * are to be processed so we know when to publish results.
 */
@Getter @EqualsAndHashCode
public class BatchMessage implements Message<PropertyMessage[]> {
    private final UUID uuid; // Should be unique per request
    private final int partitionID;
    private final long timestamp;
    private final Query query;
    private final PropertyMessage[] data;

    @JsonCreator
    public BatchMessage(@JsonProperty("uuid") final UUID uuid,
                        @JsonProperty("sourceID") final int partitionID,
                        @JsonProperty("timestamp") final long timestamp,
                        @JsonProperty("query") final Query query,
                        @JsonProperty("data") final PropertyMessage[] data) {
        this.uuid = Preconditions.checkNotNull(uuid, "uuid must not be null");
        Preconditions.checkArgument(partitionID >= 0, "Kafka partitions must be non-negative");
        this.partitionID = partitionID;
        Preconditions.checkArgument(timestamp >= 0, "timestamp must be greater than zero");
        this.timestamp = timestamp;
        this.query = Preconditions.checkNotNull(query, "query must not be null");
        this.data = Preconditions.checkNotNull(data, "messages must not be null");
    }

}
