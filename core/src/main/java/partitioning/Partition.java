package partitioning;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.Getter;

import java.io.Serializable;


@Getter
public final class Partition implements Serializable {
    private final String value;

    @JsonCreator
    public Partition(@JsonProperty("value") final String value) {
        this.value = Preconditions.checkNotNull(value, "value must not be null");
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof Partition)) {
            return false;
        }
        final Partition other = (Partition) obj;
        return this.value.equals(other.value);
    }
}
