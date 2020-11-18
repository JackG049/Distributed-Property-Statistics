package message;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

/**
 * Standard interface for a Message that is to be sent/received at any point in the system.
 *
 * The type variable T refers to what the Type of the data being store is. The timestamp is used for bucketing in the
 * statistics processor.
 */

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include= JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(name="batch", value=BatchMessage.class),
        @JsonSubTypes.Type(name="property", value=PropertyMessage.class)})
public interface Message<T> extends Serializable {
    T getData();
    long getTimestamp();
}
