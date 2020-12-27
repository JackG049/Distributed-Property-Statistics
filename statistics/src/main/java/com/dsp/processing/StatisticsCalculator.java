package com.dsp.processing;

import com.google.common.collect.ImmutableMap;
import message.PropertyMessage;
import model.StatisticsResult;
import partitioning.Partition;
import partitioning.Partitioner;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Class responsible for calculating the statistics for a given set of {@link message.PropertyMessage}s. It does so
 * by first further partitioning the data further based on the date associated with it. Then it utilizes Welford's online
 * algorithm for calculating the mean, variance and standard deviation for the dataset. The median is then calculated
 * separately.
 */
public class StatisticsCalculator {
    static final Function<List<PropertyMessage>, Double> median = (messages) -> messages.stream()
            .sorted(Comparator.comparingDouble(PropertyMessage::getPropertyPrice))
            .collect(Collectors.toList()).get(messages.size() / 2).getPropertyPrice();

    private final Partitioner datePartitioner = new Partitioner("{year}_{month}");

    /**
     * Calculates the statistics for the given set of messages.
     * @param globalPartition Refers to the top level of partition, which is created based on the {@link model.Query}.
     * @param messages The list of {@link message.PropertyMessage}s for which the statistics will be calculated.
     * @return A {@link model.StatisticsResult} containing the statistics for the partitioned data.
     */
    public StatisticsResult calculateStatistics(final Partition globalPartition, final List<PropertyMessage> messages) {
        final Map<Partition, List<PropertyMessage>> bucketedMessages = new HashMap<>();

        // partition based on localdate ({year}_{month})
        messages.forEach(message -> {
            final Partition partition = datePartitioner.partition(ImmutableMap.of(
                    "month", message.getLocalDate().getMonthValue(),
                    "year", message.getLocalDate().getYear()
            ));
            bucketedMessages.computeIfAbsent(partition, k -> new ArrayList<>()).add(message);
        });

        // Final results map with keys in the format of {prev_partitioning}_{year}_{month},
        // e.g {county}_{type}_{year}_{month}
        final Map<Partition, Map<String, Double>> results = new HashMap<>();
        for (final Map.Entry<Partition, List<PropertyMessage>> entry : bucketedMessages.entrySet()) {
            final Map<String, Double> statisticsResults = new HashMap<>();
            final OnlineStatistics onlineStatistics = new OnlineStatistics();
            for (final PropertyMessage message : entry.getValue()) {
                onlineStatistics.update(message.getPropertyPrice());
            }
            statisticsResults.put("mean", onlineStatistics.mean());
            statisticsResults.put("median", median.apply(entry.getValue()));
            statisticsResults.put("variance", onlineStatistics.variance());
            statisticsResults.put("stddev", onlineStatistics.stddev());

            results.put(Partition.join(globalPartition, new Partition(entry.getKey().getValue())), statisticsResults);
        }
        return new StatisticsResult(results);
    }

    // Online method of doing statistics, should be faster since rather than doing 3 iterations for each, mean, variance and stddev we do just 1
    static class OnlineStatistics {
        private int n;
        private double sum;
        private double mean;

        public void update(final double x) {
            ++n;
            double delta = mean + (x - mean) / n;
            sum += (x - mean) * (x - delta);
            mean = delta;
        }

        public double variance() {
            return n > 0 ? sum / n : 0.0;
        }

        public double stddev() {
            return n > 0 ? Math.sqrt(variance()) : 0.0;
        }

        public double mean() {
            return mean;
        }
    }

}
