package com.dsp.processing;

import com.dsp.processing.StatisticsProcessor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import message.PropertyMessage;
import model.PropertyData;
import model.Query;
import model.StatisticsResult;
import org.junit.Test;
import partitioning.Partition;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StatisticsProcessorTest {

    @Test
    public void applyWorksCorrectly() {
        final Map<Partition, Map<String, Double>> map = new HashMap<>();
        final Map<String, Double> statistics = new HashMap<>();
        statistics.put("stddev", Math.sqrt(3137555555.5556));
        statistics.put("mean", 170666.6667);
        statistics.put("median", 175000.0);
        statistics.put("variance", 3137555555.5556);
        map.put(new Partition("Mayo_house_1970_1"), statistics);

        final List<PropertyMessage> messages = ImmutableList.of(
                new PropertyMessage(1, LocalDate.EPOCH, new PropertyData("Mayo", "house", 100000, "M", ImmutableMap.of())),
                new PropertyMessage(1, LocalDate.EPOCH, new PropertyData("Mayo", "house", 175000, "M", ImmutableMap.of())),
                new PropertyMessage(1, LocalDate.EPOCH, new PropertyData("Mayo", "house", 237000, "M", ImmutableMap.of()))
        );
        final StatisticsProcessor statisticsProcessor = new StatisticsProcessor(new Query("Mayo", "house", null, null, null));
        final StatisticsResult[] results = statisticsProcessor.apply(messages.toArray(new PropertyMessage[0]));
        assertEquals(1, results.length);

        Map<String, Double> resultStatistics = new ArrayList<>(results[0].getStatistics().values()).get(0);
        for (final String key : resultStatistics.keySet()) {
            final double expectedValue = statistics.get(key);
            final double actualValue = resultStatistics.get(key);
            assertEquals(expectedValue, actualValue, 0.0001);
        }
        final Partition expectedPartition = new ArrayList<>(map.keySet()).get(0);
        final Partition actualPartition = new ArrayList<>(results[0].getStatistics().keySet()).get(0);
        assertEquals(expectedPartition, actualPartition);
    }
}
