package com.dsp.message;

import com.google.common.base.Preconditions;
import lombok.Getter;
import model.StatisticsResult;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * Abstract class which is implemented by the {@link com.dsp.message.ConsumerHandler} and the
 * {@link com.dsp.message.ProducerHandler}. Abstract class forces the constructor below to be called and enables the
 * sharing of a results map between the two different kafka handlers.
 */
@Getter
public abstract class KafkaHandler implements Runnable {

    /**
     * Map used to store the results of calculating statistics on {@link message.BatchMessage}s. Keys are a Pair of the
     * UUID of the {@link message.BatchMessage} and the partitionID which will be used by the client. The values are
     * then the {@link model.StatisticsResult} of the {@link message.BatchMessage}.
     */
    private final Map<Pair<UUID, Integer>, StatisticsResult[]> results;

    public KafkaHandler(final ConcurrentMap<Pair<UUID, Integer>, StatisticsResult[]> results) {
        this.results = Preconditions.checkNotNull(results, "results must not be null");
    }

    public boolean hasResults() {
        return !results.isEmpty();
    }
}
