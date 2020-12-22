package broker;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import message.RequestMessage;

import java.util.HashMap;
import java.util.UUID;

@RestController
public class BrokerService {

    private HashMap<UUID,RequestMessage> messageList;

    //brokers on different ports in dockerized version
    @PostMapping("/broker")
    public void addGateway(@RequestBody RequestMessage message) {
        messageList.put(message.getUuid(),message);
        //insert thomas's puller method
        System.out.println(messageList.get(message.getUuid()));
    }
}