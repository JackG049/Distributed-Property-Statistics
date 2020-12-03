package partitioning;

import com.google.common.collect.ImmutableMap;
import message.PropertyMessage;
import model.PropertyData;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PartitionerTest {
    private Partitioner partitioner;

    @Before
    public void setup() {
        this.partitioner = new Partitioner("The house price is {price}");
    }

    @Test
    public void testPartitionsMessageCorrectly() {
        final Partition expectedPartition = new Partition("The house price is 20.0");
        final PropertyMessage message = new PropertyMessage(Instant.now().toEpochMilli(), LocalDate.now(), new PropertyData("mayo","test", 20.0, "postcode"));
        final Partition partition = partitioner.partition(message);
        assertEquals(partition.getValue(), expectedPartition.getValue());
    }

    @Test
    public void testPartitionsCorrectly() {
        final Partition expectedPartition = new Partition("The house price is 20.0");
        final Map<String, Object> data = ImmutableMap.of("price", 20.0);
        final Partition partition = partitioner.partition(data);
        assertEquals(partition.getValue(), expectedPartition.getValue());
    }
}
