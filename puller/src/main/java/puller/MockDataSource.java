package puller;

import com.google.common.collect.ImmutableMap;
import message.PropertyMessage;
import model.PropertyData;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;

// todo include the Generator code
public class MockDataSource {
    private final int MAX_RETURNED_LISTINGS = 20;


    public PropertyMessage[] getPropertyListings(String source, String startDate, String endDate) {
        int numResults = Math.max(1, (int) ((MAX_RETURNED_LISTINGS) * Math.random()));
        PropertyMessage[] results = new PropertyMessage[numResults];

        final String[] counties = {"Antrim", "Armagh", "Carlow", "Cavan", "Clare", "Cork", "Derry", "Donegal", "Down",
                "Dublin", "Fermanagh", "Galway", "Kerry", "Kildare", "Kilkenny", "Laois", "Leitrim", "Limerick", "Longford", "Louth", "Mayo",
                "Meath", "Monaghan", "Offaly", "Roscommon", "Sligo", "Tipperary", "Tyrone", "Waterford", "Westmeath", "Wexford", "Wicklow"};

        final List<LocalDate> dates = getDatesBetweenUsing(startDate, endDate);


        for (int i = 0; i < numResults; i++) {
            String county = counties[(int) (Math.random() * 25)];  // chose county uniformly at random
            LocalDate date = dates.get((int) (Math.random() * (dates.size()-1))); // chose date uniformly at random
            String propertyType = "house";
            if (Math.random() < .35) {
                propertyType = "apartment";
            }

            results[i] = new PropertyMessage(
                    System.currentTimeMillis(), date, new PropertyData(county, propertyType, 1000.0, "X00", ImmutableMap.of())
            );
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
