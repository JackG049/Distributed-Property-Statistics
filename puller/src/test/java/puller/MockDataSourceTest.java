package puller;

import message.PropertyMessage;
import org.junit.Test;

import java.time.LocalDate;

import static org.junit.Assert.assertEquals;

public class MockDataSourceTest {
    private MockDataSource mockDataSource = new MockDataSource();

    @Test
    public void getMockListingsTest() {
        LocalDate today = LocalDate.now();
        String nowStr = today.toString();
        PropertyMessage[] listingData = mockDataSource.getPropertyListings("daft", nowStr, nowStr);
        assertEquals(listingData[0].getLocalDate(), LocalDate.now());
    }

}
