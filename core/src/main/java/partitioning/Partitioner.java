package partitioning;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import message.PropertyMessage;
import model.Query;
import org.apache.commons.text.StringSubstitutor;

import java.util.Map;

/**
 * Basic Partitioner which can be used for creating Partitions based on the template provided. Templates provided are just
 * strings that have { } surrounding the entry to be replaced. Inside the  {} should be the key from the map containing the desired value to substitute in
 *
 * An example of this is(More in the PartitionerTest class):
 *
 * String template = "{county}_{postcode}"
 * Given a map that looks like
 * {
 *     "county": "Dublin",
 *     "postcode": "D04K838"
 * }
 * would result in a Partition with a value "Dublin_D04K838"
 */
public class Partitioner {
    private final String template;

    public Partitioner(@JsonProperty("template") final String template) {
        this.template = Preconditions.checkNotNull(template, "template must not be null");
    }

    public Partition partition(final PropertyMessage message) {
        return new Partition(StringSubstitutor.replace(template, message.getData().asMap(), "{", "}"));
    }

    public Partition partition(final Map<String, ?> data) {
        return new Partition(StringSubstitutor.replace(template, data, "{", "}"));
    }

    public Partition partition(final Query query) {
        return new Partition(StringSubstitutor.replace(template, query.asMap(), "{", "}"));
    }
}
