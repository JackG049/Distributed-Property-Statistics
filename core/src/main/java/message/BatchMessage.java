package message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;

/**
 * Since the statistics service isn't
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
