package util;

import com.amazonaws.services.dynamodbv2.document.Item;
import message.PropertyMessage;
import model.PropertyData;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Map;
import java.util.Set;

public class DynamoDbUtil {

    public static PropertyData propertyItemToPropertyData(Item propertyItem) {
        String county = propertyItem.getString("County");
        double price = propertyItem.getNumber("Price").doubleValue();

        String propertyType = "house";
        if (propertyItem.isPresent("ListingType")) {
            propertyType = propertyItem.getString("ListingType");
        }

        String postcode = "000 0000";
        if (propertyItem.isPresent("Postcode")) {
            postcode = propertyItem.getString("Postcode");
        }

        Map<String, Object> additionalPropertyData = propertyItem.asMap();
        additionalPropertyData.remove("County");
        additionalPropertyData.remove("Price");
        additionalPropertyData.remove("ListingType");
        additionalPropertyData.remove("Postcode");

        return new PropertyData(county, propertyType, price, postcode, additionalPropertyData);
    }

    public static PropertyMessage propertyItemToPropertyMessage(Item propertyItem) {
        PropertyData propertyData = propertyItemToPropertyData(propertyItem);
        String listingDate = propertyItem.getString("ListingDate");

        return new PropertyMessage(LocalTime.now().toSecondOfDay(), LocalDate.parse(listingDate), propertyData);
    }

    public static Item propertyDataToPropertyItem(PropertyData propertyData) {
        Item propertyItem = new Item();

        propertyItem.withString("County", propertyData.getCounty());
        propertyItem.withNumber("Price", propertyData.getPrice());
        propertyItem.withString("ListingType", propertyData.getPropertyType());
        propertyItem.withString("Postcode", propertyData.getPostcode());

        Set<Map.Entry<String, Object>> additionalData = propertyData.getAdditionalPropertyData().entrySet();

        for (Map.Entry<String, Object> entry : additionalData) {
            if (entry.getValue() instanceof String) {
                propertyItem.withString(entry.getKey(), (String) entry.getValue());
            } else if (entry.getValue() instanceof Number) {
                propertyItem.withNumber(entry.getKey(), (Number) entry.getValue());
            }
        }

        return propertyItem;
    }

    public static Item propertyMessageToPropertyItem(PropertyMessage propertyMessage) {
        Item propertyItem = propertyDataToPropertyItem(propertyMessage.getData());
        propertyItem.withString("ListingDate", propertyMessage.getLocalDate().toString());

        return propertyItem;
    }

}
