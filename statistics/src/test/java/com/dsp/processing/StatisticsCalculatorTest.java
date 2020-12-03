package com.dsp.processing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import message.PropertyMessage;
import model.PropertyData;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

public class StatisticsCalculatorTest {
    private static List<PropertyMessage> dummyMessages;

    @BeforeClass
    public static void setup() throws IOException {
        dummyMessages = loadDataFromFile("dummy_data.csv");
    }

    @Test
    public void testMean() {
        final Function<List<PropertyMessage>, Double> mean = StatisticsCalculator.statisticsFunctions.get("mean");
        final List<PropertyMessage> messages = ImmutableList.of(
                new PropertyMessage(1, LocalDate.now(), new PropertyData("Mayo", "house", 100, "M", ImmutableMap.of())),
                new PropertyMessage(1, LocalDate.now(), new PropertyData("Mayo", "house", 150, "M", ImmutableMap.of())),
                new PropertyMessage(1, LocalDate.now(), new PropertyData("Mayo", "house", 125, "M", ImmutableMap.of()))
        );
        assertEquals(125.0, mean.apply(messages), 0.01);
    }

    @Test
    public void testMedian() {
        final Function<List<PropertyMessage>, Double> median = StatisticsCalculator.statisticsFunctions.get("median");
        final List<PropertyMessage> messages = ImmutableList.of(
                new PropertyMessage(1, LocalDate.now(), new PropertyData("Mayo", "house", 100, "M", ImmutableMap.of())),
                new PropertyMessage(1, LocalDate.now(), new PropertyData("Mayo", "house", 150, "M", ImmutableMap.of())),
                new PropertyMessage(1, LocalDate.now(), new PropertyData("Mayo", "house", 75, "M", ImmutableMap.of()))
        );
        assertEquals(100.0, median.apply(messages), 0.01);
    }

    @Test
    public void testVariance() {
        final Function<List<PropertyMessage>, Double> variance = StatisticsCalculator.statisticsFunctions.get("variance");
        final List<PropertyMessage> messages = ImmutableList.of(
                new PropertyMessage(1, LocalDate.now(), new PropertyData("Mayo", "house", 100, "M", ImmutableMap.of())),
                new PropertyMessage(1, LocalDate.now(), new PropertyData("Mayo", "house", 150, "M", ImmutableMap.of())),
                new PropertyMessage(1, LocalDate.now(), new PropertyData("Mayo", "house", 125, "M", ImmutableMap.of()))
        );
        assertEquals(416.6667, variance.apply(messages), 0.0001);
    }

    @Test
    public void test() {
        final Function<List<PropertyMessage>, Double> variance = StatisticsCalculator.statisticsFunctions.get("variance");
        final List<PropertyMessage> messages = ImmutableList.of(
                new PropertyMessage(1, LocalDate.now(), new PropertyData("Mayo", "house", 100000, "M", ImmutableMap.of())),
                new PropertyMessage(1, LocalDate.now(), new PropertyData("Mayo", "house", 175000, "M", ImmutableMap.of())),
                new PropertyMessage(1, LocalDate.now(), new PropertyData("Mayo", "house", 237000, "M", ImmutableMap.of()))
        );
        System.out.println(variance.apply(messages));
        assertEquals(3137555555.5556, variance.apply(messages), 0.001);
    }

    @Test
    public void testStddev() {
        final Function<List<PropertyMessage>, Double> stddev = StatisticsCalculator.statisticsFunctions.get("stddev");
        final List<PropertyMessage> messages = ImmutableList.of(
                new PropertyMessage(1, LocalDate.now(), new PropertyData("Mayo", "house", 100, "M", ImmutableMap.of())),
                new PropertyMessage(1, LocalDate.now(), new PropertyData("Mayo", "house", 150, "M", ImmutableMap.of())),
                new PropertyMessage(1, LocalDate.now(), new PropertyData("Mayo", "house", 125, "M", ImmutableMap.of()))
        );
        assertEquals(Math.sqrt(416.6667), stddev.apply(messages), 0.0001);
    }

    private static List<PropertyMessage> loadDataFromFile(final String fileName) throws IOException {
        final InputStream in = StatisticsCalculatorTest.class.getClassLoader().getResourceAsStream(fileName);
        final BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line;
        final List<PropertyMessage> messages = new ArrayList<>();
        while ((line = br.readLine()) != null) {
            String args[] = line.split(";");
            final long timestamp = Instant.now().toEpochMilli();
            final String[] dateStrings = args[3].replace("[", "").replace("]", "").split(",");
            final LocalDate date = LocalDate.of(Integer.parseInt(dateStrings[0]), Integer.parseInt(dateStrings[1]), Integer.parseInt(dateStrings[2]));

            final String county = args[0];
            final String postcode = args[1];
            final double price = Double.parseDouble(args[2]);
            final PropertyMessage message =
                    new PropertyMessage(timestamp, date, new PropertyData(county, "house", price, postcode, ImmutableMap.of()));
            messages.add(message);
        }
        in.close();
        br.close();
        return messages;
    }
}
