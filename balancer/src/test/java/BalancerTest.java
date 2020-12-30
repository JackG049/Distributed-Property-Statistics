import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import balancer.BalancerService;

public class BalancerTest {
    private BalancerService balancerService = new BalancerService();

    @Test
    public void healthTest() {
        assertNotNull(balancerService);
    }
}
