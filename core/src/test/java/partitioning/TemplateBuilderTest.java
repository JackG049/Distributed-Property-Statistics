package partitioning;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TemplateBuilderTest {

    @Test
    public void buildSingleArgumentTemplate() {
        TemplateBuilder templateBuilder = new TemplateBuilder();
        String template = templateBuilder.withCounty(true).build();
        assertEquals("{county}", template);

        templateBuilder = new TemplateBuilder();
        template = templateBuilder.withPostcodePrefix(true).build();
        assertEquals("{postcodePrefix}", template);

        templateBuilder = new TemplateBuilder();
        template = templateBuilder.withPropertyType(true).build();
        assertEquals("{propertyType}", template);
    }

    @Test
    public void buildBiArgumentTemplate() {
        TemplateBuilder templateBuilder = new TemplateBuilder();
        String template = templateBuilder.withCounty(true).withPropertyType(true).build();
        assertEquals("{county}_{propertyType}", template);
    }

    @Test
    public void buildAllArgumentTemplate() {
        TemplateBuilder templateBuilder = new TemplateBuilder();
        String template = templateBuilder.withCounty(true).withPropertyType(true).withPostcodePrefix(true).build();
        assertEquals("{county}_{propertyType}_{postcodePrefix}", template);

        templateBuilder = new TemplateBuilder();
        template = templateBuilder.withPostcodePrefix(true).withCounty(true).withPropertyType(true).build();
        assertEquals("{postcodePrefix}_{county}_{propertyType}", template);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsExceptionWithAllNull() {
        final TemplateBuilder templateBuilder = new TemplateBuilder();
        templateBuilder.build();
    }
}
