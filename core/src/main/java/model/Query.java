package model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class Query {
    private final String county;
    private final String propertyType;
    private final String postcodePrefix;
    // Using the wrapper version so they can be set to null
    private final Double minPrice;
    private final Double maxPrice;

    @JsonCreator
    public Query(@JsonProperty("county") final String county,
                 @JsonProperty("propertyType") final String propertyType,
                 @JsonProperty("postcodePrefix") final String postcodePrefix,
                 @JsonProperty("minPrice") final Double minPrice,
                 @JsonProperty("maxPrice") final Double maxPrice) {
        this.county = county;
        this.propertyType = propertyType;
        this.postcodePrefix = postcodePrefix;
        this.minPrice = minPrice;
        this.maxPrice = maxPrice;
    }

}
