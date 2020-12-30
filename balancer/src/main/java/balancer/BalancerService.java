package balancer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import message.RequestMessage;
import rest.UrlConstants;

/**
 * BalancerService Distributes Requests via round robin
 */
@RestController
public class BalancerService {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(BalancerService.class);
    private int offset = 0;

    @PostMapping("/client")
    public void distribute(@RequestBody RequestMessage message) {
        LOGGER.info("Message Recieved...");
        loadBalancer(message);
    }

    private void loadBalancer(RequestMessage message) {
        RestTemplate restTemplate = new RestTemplate();
        HttpEntity<RequestMessage> request = new HttpEntity<>(message);
        String brokerport = brokerPort();
        restTemplate.postForObject(brokerport, request, RequestMessage.class);
        LOGGER.info("Message Sent to " + brokerport);
    }

    private String brokerPort() {
        return "http://puller:" + (8082 + offset++%UrlConstants.PullerInstances) + "/query";
    }
}