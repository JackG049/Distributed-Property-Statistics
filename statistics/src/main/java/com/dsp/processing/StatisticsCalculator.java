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


public class StatisticsCalculator {
    // Will probably change to a BiFunction so as to include property key to look up, e.g. get statistics for area etc
    static volatile Map<String, Function<List<PropertyMessage>, Double>> statisticsFunctions = new HashMap<>();

    static {
        statisticsFunctions.put("mean", propertyMessages -> propertyMessages.stream().mapToDouble(PropertyMessage::getPropertyPrice).reduce(0.0, Double::sum) / propertyMessages.size());
        statisticsFunctions.put("median", propertyMessages -> propertyMessages.stream()
                .sorted(Comparator.comparingDouble(PropertyMessage::getPropertyPrice))
                .collect(Collectors.toList()).get(propertyMessages.size() / 2).getPropertyPrice());
        statisticsFunctions.put("variance", propertyMessages -> {
            double mean = statisticsFunctions.get("mean").apply(propertyMessages);
            return propertyMessages.stream().mapToDouble(PropertyMessage::getPropertyPrice).map(price -> Math.pow(price - mean, 2)).reduce(0.0, Double::sum) / propertyMessages.size();
        });
        statisticsFunctions.put("stddev", propertyMessages ->  Math.sqrt(statisticsFunctions.get("variance").apply(propertyMessages)));
    }

    private final Partitioner datePartitioner = new Partitioner("{year}_{month}");

    public StatisticsResult calculateStatistics(final Partition global, final List<PropertyMessage> messages) {
        final Map<Partition, List<PropertyMessage>> bucketedMessages = new HashMap<>();

        // bucket based on localdate ({year}_{month})
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
            for (final Map.Entry<String, Function<List<PropertyMessage>, Double>> functionEntry : statisticsFunctions.entrySet()) {
                statisticsResults.put(functionEntry.getKey(), functionEntry.getValue().apply(entry.getValue()));
            }
            results.put(Partition.join(global, new Partition(entry.getKey().getValue())), statisticsResults);
        }
        return new StatisticsResult(results);
    }
}
