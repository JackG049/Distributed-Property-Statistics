package message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.Getter;
import model.StatisticsResult;

import java.util.UUID;

/**
 * Message implementation which stores the result from the Statistics service
 */
@Getter
public class ResultsMessage implements Message<StatisticsResult[]> {
    private final UUID uuid;
    private final int partitionID;
    private final StatisticsResult[] data;
    private final long timestamp;

    @JsonCreator
    public ResultsMessage(@JsonProperty("uuid") final UUID uuid,
                          @JsonProperty("partitionID") final int partitionID,
                          @JsonProperty("timestamp") final long timestamp,
                          @JsonProperty("data") final StatisticsResult[] data) {
        this.uuid = Preconditions.checkNotNull(uuid, "uuid must not be null");
        Preconditions.checkArgument(partitionID >= 0, "partitionID must be non-negative");
        this.partitionID = partitionID;
        this.timestamp = timestamp;
        this.data = Preconditions.checkNotNull(data, "data must not be null");
    }
}
