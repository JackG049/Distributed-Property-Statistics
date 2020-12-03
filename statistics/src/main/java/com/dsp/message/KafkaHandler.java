package com.dsp.message;

import com.google.common.base.Preconditions;
import lombok.Getter;
import model.StatisticsResult;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Getter
public abstract class KafkaHandler implements Runnable {
    private final Map<Pair<UUID, Integer>, StatisticsResult[]> results;

    public KafkaHandler(final ConcurrentMap<Pair<UUID, Integer>, StatisticsResult[]> results) {
        this.results = Preconditions.checkNotNull(results, "results must not be null");
    }

    public boolean hasResults() {
        return !results.isEmpty();
    }
}
