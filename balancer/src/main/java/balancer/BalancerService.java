package balancer;

import org.springframework.http.HttpEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import message.RequestMessage;
import rest.UrlConstants;

import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
public class BalancerService {
    private int offset = 0;
    private LinkedList<RequestMessage> messageList = new LinkedList<RequestMessage>();
    private static final Logger LOGGER = LoggerFactory.getLogger(BalancerService.class);

    @PostMapping("/client")
    public void addGateway(@RequestBody RequestMessage message) {
        messageList.add(message);
        System.out.println(messageList.get(0).getPartitionID());
        loadBalancer(message);
    }

    //Send requests in round robin
    private void loadBalancer(RequestMessage message) {
        LOGGER.info("Request Recieved...");
        RestTemplate restTemplate = new RestTemplate();
        HttpEntity<RequestMessage> request = new HttpEntity<>(message);
        restTemplate.postForObject(brokerPort(), request, RequestMessage.class);
        LOGGER.info("Sending to Puller");
    }

    private String brokerPort() {
        return "http://192.168.99.101:8082/query";// + (8082/query%UrlConstants.PullerInstances);
    }
}