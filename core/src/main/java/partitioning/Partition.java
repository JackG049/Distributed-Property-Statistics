package partitioning;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Wrapper class around a String which is used by a {@link partitioning.Partitioner}.
 */
@Getter @EqualsAndHashCode
public final class Partition implements Serializable {
    private final String value;

    @JsonCreator
    public Partition(@JsonProperty("value") final String value) {
        this.value = Preconditions.checkNotNull(value, "value must not be null");
    }

    /**
     * Method which can be used to join any number of partitions together to create a new partition based on those passed.
     * @param partitions Variable length input of Partitions to be joined
     * @return "_" separated partition consisting of the partitions passed
     */
    public static Partition join(final Partition ...partitions) {
        return new Partition(Arrays.stream(partitions).map(Partition::toString).collect(Collectors.joining("_")));
    }

    @Override
    public String toString() {
        return value;
    }
}
