package model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Map;
/**
 * Represents the data relating to a single property
 */
@Getter @EqualsAndHashCode
public final class PropertyData {
    private final String county;
    private final String propertyType;
    private final double price;
    private final String postcode;

    private final Map<String, Object> additionalPropertyData;

    @JsonCreator
    public PropertyData(@JsonProperty("county") final String county,
                        @JsonProperty("propertyType") final String propertyType,
                        @JsonProperty("price") final double price,
                        @JsonProperty("postcode") final String postcode,
                        @JsonProperty("additionalPropertyData") final Map<String, Object> additionalPropertyData) {
        this.county = Preconditions.checkNotNull(county, "county must not be null");
        this.propertyType = Preconditions.checkNotNull(propertyType, "propertyType must not be null");
        Preconditions.checkArgument(price >= 0.0, "price must be greater than or equal to zero");
        this.price = price;
        this.postcode = Preconditions.checkNotNull(postcode, "postcode must not be null");
        this.additionalPropertyData = additionalPropertyData;
    }

    public PropertyData(final String county, final String propertyType, final double price, final String postcode) {
        this(county, propertyType, price, postcode, ImmutableMap.of());
    }

    @JsonIgnore
    public <S> S getAdditionalProperty(final String key) {
        return (S) additionalPropertyData.get(key);
    }

    @JsonIgnore
    public Map<String, Object> asMap() {
        return ImmutableMap.<String, Object>builder().put("propertyType", propertyType).put("price", price).put("county", county).put("postcode", postcode).putAll(additionalPropertyData).build();
    }
}
