package puller;

import com.google.common.collect.ImmutableMap;
import counties.County;
import message.PropertyMessage;
import model.PropertyData;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

// todo include the Generator code
public class MockDataSource {
    private final int MAX_RETURNED_LISTINGS = 20;
    private AtomicLong listingId = new AtomicLong(0L);

    public Map<String, PropertyMessage> getPropertyListings(String source, String startDate, String endDate) {
        int numResults = Math.max(1, (int) ((MAX_RETURNED_LISTINGS) * Math.random()));
        Map<String, PropertyMessage> results = new HashMap<>();

        final List<String> counties = County.getCounties();
        final List<LocalDate> dates = getDatesBetweenUsing(startDate, endDate);


        for (int i = 0; i < numResults; i++) {
            String county = counties.get((int) (Math.random() * 31));  // chose county uniformly at random
            LocalDate date = dates.get((int) (Math.random() * (dates.size()-1))); // chose date uniformly at random
            String propertyType = "house";
            if (Math.random() < .35) {
                propertyType = "apartment";
            }

            results.put(source + "_" + listingId.getAndIncrement(),  new PropertyMessage(
                    System.currentTimeMillis(), date, new PropertyData(county, propertyType, 1000.0, "X00", ImmutableMap.of())
            ));
        }

        return results;
    }


    public static List<LocalDate> getDatesBetweenUsing(String startDateStr, String endDateStr) {
        LocalDate startDate = LocalDate.parse(startDateStr);
        LocalDate endDate = LocalDate.parse(endDateStr);

        List<LocalDate> dates = startDate.datesUntil(endDate).collect(Collectors.toList());
        dates.add(endDate);
        return dates;
    }
}
