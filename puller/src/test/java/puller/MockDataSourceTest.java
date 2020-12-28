package puller;

import message.PropertyMessage;
import org.junit.Test;

import java.time.LocalDate;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MockDataSourceTest {
    private MockDataSource mockDataSource = new MockDataSource();

    @Test
    public void getMockListingsTest() {
        LocalDate today = LocalDate.now();
        String nowStr = today.toString();
        Map<String, PropertyMessage> listingData = mockDataSource.getPropertyListings("daft", nowStr, nowStr);

        assertTrue(!listingData.isEmpty());
        PropertyMessage propertyMessage = listingData.entrySet().iterator().next().getValue();
        assertEquals(propertyMessage.getLocalDate(), LocalDate.now());
    }

}
