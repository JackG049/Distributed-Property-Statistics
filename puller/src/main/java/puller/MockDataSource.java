package puller;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import message.PropertyMessage;
import model.PropertyData;
import model.Query;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import util.Util;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * MockDataSource mocks a remote property database. It generates sample property listings.
 */
public class MockDataSource {
    private final int MAX_PULLED_LISTINGS = 2;
    private final int MAX_TEST_LISTINGS = 50;
    private AtomicLong listingId = new AtomicLong(0L);
    private static final String[] PROPERTY_TYPE = {"house", "apartment"};

    /**
     * Generate sample property listings within a date range
     * @param dataSourceName e.g daft, myhomes
     * @return sample property listings within the specified date range
     */
    public Map<String, PropertyMessage> getPropertyListings(String dataSourceName, Query query) {
        String startDate = query.getStartDate();
        String endDate = query.getEndDate();
        Map<String, PropertyMessage> propertyListings = new HashMap<>();

        final List<LocalDate> dates = getDatesBetween(startDate, endDate);
        List<PropertyData> samplePropertyData;
        try {
            samplePropertyData = generatePropertyData(MAX_TEST_LISTINGS, query);
        } catch (IOException e) {
            System.err.println("Failed to generate property data");
            e.printStackTrace();
            return propertyListings;
        }


        for (PropertyData propertyData : samplePropertyData) {
            LocalDate date = dates.get((int) (Math.random() * (dates.size()-1))); // chose date uniformly at random
            propertyListings.put(dataSourceName + "_" + listingId.getAndIncrement(),  new PropertyMessage(
                    System.currentTimeMillis(), date, propertyData)
            );
        }

        return propertyListings;
    }
    public Map<String, PropertyMessage> getPropertyListings(String dataSourceName, String startDate, String endDate ) {
        Map<String, PropertyMessage> propertyListings = new HashMap<>();

        final List<LocalDate> dates = getDatesBetween(startDate, endDate);
        List<PropertyData> samplePropertyData;
        try {
            samplePropertyData = generateGeneralPropertyData(MAX_PULLED_LISTINGS);
        } catch (IOException e) {
            System.err.println("Failed to generate property data");
            e.printStackTrace();
            return propertyListings;
        }


        for (PropertyData propertyData : samplePropertyData) {
            LocalDate date = dates.get((int) (Math.random() * (dates.size() - 1))); // chose date uniformly at random
            propertyListings.put(dataSourceName + "_" + listingId.getAndIncrement(), new PropertyMessage(
                    System.currentTimeMillis(), date, propertyData)
            );
        }

        return propertyListings;
    }



    private static List<LocalDate> getDatesBetween(String startDateStr, String endDateStr) {
        LocalDate startDate = LocalDate.parse(startDateStr);
        LocalDate endDate = LocalDate.parse(endDateStr);

        List<LocalDate> dates = startDate.datesUntil(endDate).collect(Collectors.toList());
        dates.add(endDate);
        return dates;
    }

    private static List<PropertyData> generatePropertyData(long numberProperties, Query query) throws IOException {
        final ObjectMapper objectMapper = Util.objectMapper;
        final InputStream inputStream = MockDataSource.class.getClassLoader().getResourceAsStream("counties.json");
        if (inputStream == null) {
            System.exit(1);
        }
        final Map<String, Double> countiesPriceMap = objectMapper.readValue(inputStream.readAllBytes(), Map.class);
        final Map<String, Double> countyApartmentPrices = createApartmentPrices(countiesPriceMap);

        double min = 0.75;
        double max = 1.25;
        double diff = max - min;
        final List<PropertyData> dataList = new ArrayList<>();
        String type = query.getPropertyType();
        String county = query.getCounty();

        for (int i = 0; i < numberProperties; i++) {
            if (type.equals("house")) {
                double multiplier = (((float) Math.random() / max) * diff) + min;
                final double price = (double) Math.round(countiesPriceMap.get(county) * multiplier);
                dataList.add(new PropertyData(county, type, price, "xxx_xxxx"));
            } else if (type.equals("apartment")) {
                double multiplier = (((float) Math.random() / max) * diff) + min;
                final double price = (double) Math.round(countyApartmentPrices.get(county) * multiplier);
                dataList.add(new PropertyData(county, type, price, "xxx_xxxx"));
            }
        }

        return dataList;
    }

    private static List<PropertyData> generateGeneralPropertyData(long numberProperties) throws IOException {
        final ObjectMapper objectMapper = Util.objectMapper;
        final InputStream inputStream = MockDataSource.class.getClassLoader().getResourceAsStream("counties.json");
        if (inputStream == null) {
            System.exit(1);
        }
        final Map<String, Double> countiesPriceMap = objectMapper.readValue(inputStream.readAllBytes(), Map.class);
        final Map<String, Double> countyApartmentPrices = createApartmentPrices(countiesPriceMap);

        double min = 0.75;
        double max = 1.25;
        double diff = max - min;
        final List<PropertyData> dataList = new ArrayList<>();
        for (final String type : PROPERTY_TYPE) {
            for (final String county : countiesPriceMap.keySet()) {
                for (int i = 0; i < numberProperties; i++) {
                    if (type.equals("house")) {
                        double multiplier = (((float) Math.random() / max) * diff) + min;
                        final double price = (double) Math.round(countiesPriceMap.get(county) * multiplier);
                        dataList.add(new PropertyData(county, type, price, "xxx_xxxx"));
                    } else if (type.equals("apartment")) {
                        double multiplier = (((float) Math.random() / max) * diff) + min;
                        final double price = (double) Math.round(countyApartmentPrices.get(county) * multiplier);
                        dataList.add(new PropertyData(county, type, price, "xxx_xxxx"));
                    }
                }
            }
        }

        return dataList;
    }

    private static Map<String, Double> createApartmentPrices(final Map<String, Double> countiesPriceMap) {
        final Map<String, Double> apartmentMap = new HashMap<>();
        double min = 0.6;
        double max = 0.9;
        double diff = max - min;
        for (final Map.Entry<String, Double> entry : countiesPriceMap.entrySet()) {
            double multiplier =  (((float) Math.random() / 0.9) * diff) + min;
            int newVal = (int)(entry.getValue() * multiplier);
            apartmentMap.put(entry.getKey(), (double)newVal);
        }
        return apartmentMap;
    }

}
