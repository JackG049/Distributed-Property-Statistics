package model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import partitioning.Partition;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Getter
public final class StatisticsResult {
    /**
     * Map Partitioned based on query with inner map being the statistics for that partition
     */
    private final Map<Partition, Map<String, Double>> statistics;

    @JsonCreator
    public StatisticsResult(@JsonProperty("statistics") Map<Partition, Map<String, Double>> statistics) {
        this.statistics = statistics;
    }
    
    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof StatisticsResult)) {
            return false;
        }
        final StatisticsResult other = (StatisticsResult)o;

        return other.getStatistics().equals(this.getStatistics());
    }

    
}
