package model;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Class representing a user query. This information can be used to construct queries based on a number of parameters
 * which can then be graphed.
 */
@Getter
@EqualsAndHashCode
public class Query {
    private final String county;
    private final String propertyType;
    private final String postcodePrefix;
    // Using the wrapper version so they can be set to null
    private final String startDate;
    private final String endDate;
    private final Double minPrice;
    private final Double maxPrice;

    @JsonCreator
    public Query(@JsonProperty("county") final String county,
                 @JsonProperty("propertyType") final String propertyType,
                 @JsonProperty("postcodePrefix") final String postcodePrefix,
                 @JsonProperty("startDate") final String startDate,
                 @JsonProperty("endDate") final String endDate,
                 @JsonProperty("minPrice") final Double minPrice,
                 @JsonProperty("maxPrice") final Double maxPrice) {
        this.county = county;
        this.propertyType = propertyType;
        this.postcodePrefix = postcodePrefix;
        this.startDate = startDate;
        this.endDate = endDate;
        this.minPrice = minPrice;
        this.maxPrice = maxPrice;
    }

    @JsonIgnore
    public Map<String, Object> asMap() {
        return ImmutableMap.<String, Object>builder().put("county", county).put("propertyType", propertyType).put("postcodePrefix", postcodePrefix).put("startDate", startDate).put("endDate", endDate).put("minPrice", minPrice).put("maxPrice", maxPrice).build();
    }
}
