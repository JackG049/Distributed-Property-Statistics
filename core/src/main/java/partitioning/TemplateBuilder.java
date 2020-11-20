package partitioning;

import java.util.LinkedHashMap;
import java.util.Map;

public final class TemplateBuilder {
    private final Map<String, Boolean> templateMap = new LinkedHashMap<>();

    public String build() throws IllegalArgumentException {
        final StringBuilder builder = new StringBuilder();
        if (templateMap.values().stream().noneMatch(v -> v)) {
            throw new IllegalArgumentException("failed to provide any values to build template");
        }
        int counter = 0;
        int mapSize = templateMap.size();
        for (final Map.Entry<String, Boolean> entry : templateMap.entrySet()) {
            if (entry.getValue()) {
                builder.append("{").append(entry.getKey()).append("}");
                if (counter != mapSize - 1) {
                    builder.append("_");
                }
            }
            counter++;
        }
        return builder.toString();
    }

    public TemplateBuilder withCounty(final boolean value) {
        templateMap.put("county", value);
        return this;
    }

    public TemplateBuilder withPropertyType(final boolean value) {
            templateMap.put("propertyType", value);
            return this;
    }

    public TemplateBuilder withPostcodePrefix(final boolean value) {
        templateMap.put("postcodePrefix", value);
        return this;
    }
}
