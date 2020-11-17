package message;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include= JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(name="batch", value=BatchMessage.class),
        @JsonSubTypes.Type(name="property", value=PropertyMessage.class)})
public interface Message<T> extends Serializable {
    T getData();
    long getTimestamp();
}
