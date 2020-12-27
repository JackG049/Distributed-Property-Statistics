package message;

import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import model.Query;

/**
 * Message created by the client to request information on properties.
 */
@Getter
public class RequestMessage {
    private final Query query;
    private final long timestamp;
    private final UUID uuid;
    private final int partitionID;

    public RequestMessage(@JsonProperty("id") final UUID uuid,
                          @JsonProperty("partitionID") final int partitionID,
                          @JsonProperty("result") final Query query,
                          @JsonProperty("timestamp") final long timestamp) {
        this.uuid = uuid;
        this.partitionID = partitionID;
        this.query = query;
        this.timestamp = timestamp;
    }
}
