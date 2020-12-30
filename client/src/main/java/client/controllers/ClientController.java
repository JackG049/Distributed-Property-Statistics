package client.controllers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.springframework.http.HttpEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.client.RestTemplate;

import counties.County;
import message.MessageDeserializer;
import message.RequestMessage;
import model.Query;
import model.StatisticsResult;
import partitioning.Partition;
import results.ResultsHandler;
import util.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.web.bind.annotation.PostMapping;
/**
 * ClientController for client endpoints on port 8080
 */
@Controller
public class ClientController {

    private UUID uuid;
    private static final Properties props;

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientController.class);
    private static final MessageDeserializer deserializer = new MessageDeserializer(Util.objectMapper);

    static {
        props = Util.loadPropertiesFromFile("consumer.properties");
        props.setProperty("group.id",  "results_client");
    }
    private final ResultsHandler resultsHandler = new ResultsHandler(props, deserializer);
    private Thread consumerThread = new Thread(resultsHandler);
    private final int partitionId = resultsHandler.getPartitionId();

    @GetMapping("/")
    public String home(Model model) {
        model.addAttribute("Heading", "Home");
        model.addAttribute("counties", County.getCounties());
        return "home.html";
    }

    /**
     * query endpoint that takes parameters from home.html and posts to balancer
     * @param county
     * @param type
     * @param sDate
     * @param eDate
     * @param minPrice
     * @param maxPrice
     * @param model
     * @return display.html
     * @throws InterruptedException
     */
    @PostMapping("/query")
    public String query(String county, String type, String sDate, String eDate, String minPrice, String maxPrice,
            Model model) throws InterruptedException {
        model.addAttribute("Heading", "Home");
        RestTemplate restTemplate = new RestTemplate();
        Query query = new Query(county, type, "000", sDate, eDate, Double.parseDouble(minPrice),
                Double.parseDouble(maxPrice));
        uuid = UUID.randomUUID();
        RequestMessage requestMessage = new RequestMessage(uuid, partitionId, query,
                Instant.EPOCH.toEpochMilli());
        HttpEntity<RequestMessage> request = new HttpEntity<>(requestMessage);
        restTemplate.postForObject("http://balancer:8081" + "/client", request, RequestMessage.class);
        
        if(!consumerThread.isAlive())
            startThread();

        List<String> dates = new ArrayList<>();
        Map<String, Map<String,Double>> myhomeMap = new HashMap<>();
        Map<String, Map<String,Double>> daftMap = new HashMap<>();

        int count = 0;
        int lim = 2;
        Thread.sleep(3000);
        while(count != lim) {
            if(resultsHandler.isEmpty(uuid)) {
                LOGGER.info("No Results Found ... Polling");
                Thread.sleep(1500);
            }
            else {
                LOGGER.info("Results Found ... ");
                StatisticsResult[] results = resultsHandler.getResult(uuid, "daft");
                for(StatisticsResult result : results) {
                    for(Partition partition : result.getStatistics().keySet()) {
                            dates.add(parsePartition(partition));
                            daftMap.put(parsePartition(partition), result.getStatistics().get(partition));
                    }
                }
                results = resultsHandler.getResult(uuid, "myhome");
                for(StatisticsResult result : results) {
                    for(Partition partition : result.getStatistics().keySet()) {
                            myhomeMap.put(parsePartition(partition), result.getStatistics().get(partition));
                    }
                }
                //Break loop if results are empty
                count=lim;
            }
        }

        model.addAttribute("dates", dates);
        model.addAttribute("myhomeMap", myhomeMap);
        model.addAttribute("daftMap", daftMap);
        return "display.html";
    }

    /**
     * Return 
     */
    private String parsePartition(Partition partition) {
        String result = partition.getValue();
        int datePosition = result.lastIndexOf("_")-5;
        result = result.substring(datePosition+1).replaceAll("_", "-");
        return result;
    }

    private void startThread() {
        consumerThread.start();
    }
}
