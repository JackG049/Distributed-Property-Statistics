package message;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import model.StatisticsResult;

@Getter
public class ResultsMessage implements Message<StatisticsResult> {
    private final StatisticsResult data;
    private final long timestamp;
    private final long id;

    public ResultsMessage(@JsonProperty("result") final StatisticsResult data,
                          @JsonProperty("timestamp") final long timestamp,
                          @JsonProperty("id") final long id) {
        this.data = data;
        this.timestamp = timestamp;
        this.id = id;
    }
}
