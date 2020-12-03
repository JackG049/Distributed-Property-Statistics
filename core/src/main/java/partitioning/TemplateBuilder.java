package partitioning;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public final class TemplateBuilder {
    private final Map<String, Boolean> templateMap = new LinkedHashMap<>();

    public String build() throws IllegalArgumentException {
        boolean allFalse = true;
        for (final boolean b : templateMap.values()) {
            if (b) {
                allFalse = false;
                break;
            }
        }
        if (allFalse) {
            throw new IllegalArgumentException("failed to provide any values to build template");
        }

        return templateMap.entrySet().stream().filter(Map.Entry::getValue)
                .map(Map.Entry::getKey)
                .map(s -> "{" + s + "}")
                .collect(Collectors.joining("_"));
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
