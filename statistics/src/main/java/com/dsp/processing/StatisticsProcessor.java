package com.dsp.processing;

import message.PropertyMessage;
import model.Query;
import model.StatisticsResult;
import partitioning.Partition;
import partitioning.Partitioner;
import partitioning.TemplateBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class StatisticsProcessor implements Function<PropertyMessage[], StatisticsResult[]> {
    private final Partitioner partitioner;

    public StatisticsProcessor(final Query query) {
        this.partitioner = new Partitioner(buildTemplateFromQuery(query));
    }

    @Override
    public StatisticsResult[] apply(final PropertyMessage[] propertyMessages) {
        final Map<Partition, List<PropertyMessage>> partitions = new ConcurrentHashMap<>();
        for (final PropertyMessage message : propertyMessages) {
            final Partition partition = partitioner.partition(message.getData().asMap());
            partitions.computeIfAbsent(partition, k -> new ArrayList<>()).add(message);
        }

        return partitions.entrySet()
                .parallelStream()
                .map(entry -> new StatisticsCalculator().calculateStatistics(entry.getKey(), entry.getValue()))
                .toArray(StatisticsResult[]::new);
    }

    private String buildTemplateFromQuery(final Query query) {
        final String county = query.getCounty();
        final String propertyType = query.getPropertyType();
        final String postcodePrefix = query.getPostcodePrefix();

        return new TemplateBuilder()
                .withCounty(county != null)
                .withPropertyType(propertyType != null)
                .withPostcodePrefix(postcodePrefix != null)
                .build();
    }
}
