import com.fasterxml.jackson.databind.ObjectMapper;
import model.PropertyData;
import util.Util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Generator {
    private static final String[] TYPES = {"house", "apartment"};

    public static void main(final String[] args) throws IOException {
        final ObjectMapper objectMapper = Util.objectMapper;
        final InputStream inputStream = Generator.class.getClassLoader().getResourceAsStream("counties.json");
        if (inputStream == null) {
            System.exit(1);
        }
        final Map<String, Double> countiesPriceMap = objectMapper.readValue(inputStream.readAllBytes(), Map.class);
        final Map<String, Double> countyApartmentPrices = createApartmentPrices(countiesPriceMap);

        double min = 0.75;
        double max = 1.25;
        double diff = max - min;
        final List<PropertyData> dataList = new ArrayList<>();
        for (final String type : TYPES) {
            for (final String county : countiesPriceMap.keySet()) {
                for (int i = 0; i < 5000; i++) {
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
        final BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("propertyData.json"));
        bufferedWriter.write(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(dataList));
        bufferedWriter.close();
        System.exit(0);
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