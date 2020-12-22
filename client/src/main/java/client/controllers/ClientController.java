package client.controllers;

import java.time.Instant;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.http.HttpEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.client.RestTemplate;

import message.MessageDeserializer;
import message.RequestMessage;
import model.Query;
import model.StatisticsResult;
import rest.UrlConstants;
import results.ResultsHandler;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.web.bind.annotation.PostMapping;

@Controller
public class ClientController {

    private Map<Pair<UUID, Integer>,StatisticsResult[]> map;
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientController.class);
    private static final MessageDeserializer deserializer = new MessageDeserializer(new ObjectMapper());
    private final UUID uuid = UUID.randomUUID();
    private static final Properties props;
    static {
        props = loadPropertiesFromFile("consumer.properties");
        props.setProperty("group.id",  "results_client");
    }
    private final ResultsHandler resultsHandler = new ResultsHandler(props, deserializer);
    private final int partitionId = resultsHandler.getPartitionId();


    @GetMapping("/")
    public String home(Model model) {
        model.addAttribute("Heading", "Home");
        return "home.html";
    }

    @PostMapping("/query")
    public String query(String county, String type, String sDate, String eDate, String minPrice, String maxPrice,
            Model model) throws InterruptedException {
        model.addAttribute("Heading", "Home");
        RestTemplate restTemplate = new RestTemplate();
        Query query = new Query(county, type, "test", sDate, eDate, Double.parseDouble(minPrice),
                Double.parseDouble(maxPrice));
        RequestMessage requestMessage = new RequestMessage(uuid, partitionId, query,
                Instant.EPOCH.toEpochMilli());
        HttpEntity<RequestMessage> request = new HttpEntity<>(requestMessage);
        restTemplate.postForObject(UrlConstants.BALANCER, request, RequestMessage.class);
        while(map.isEmpty()) {
            if(resultsHandler.isEmpty(uuid))
                Thread.sleep(200);
            else {
                map.put(Pair.of(uuid, partitionId), resultsHandler.getResult(uuid));
            }
        }
        System.out.println(map.get(Pair.of(uuid, partitionId)));

        //todo display.html
        return "redirect:/display";
    }

    @GetMapping("/display")
    public String display(Model model) {
        return "display.html";
    }

    private static Properties loadPropertiesFromFile(final String fileName) {
        final Properties properties = new Properties();

        try (final InputStream propertiesInputStream = ClientController.class.getClassLoader().getResourceAsStream(fileName)) {
            LOGGER.info("Attempting to load properties from " + fileName + "...");
            properties.load(propertiesInputStream);
            LOGGER.info("Success");

        } catch (final IOException ex) {
            LOGGER.warn("Failure");
            ex.printStackTrace();
        }
        return properties;
    }
}
