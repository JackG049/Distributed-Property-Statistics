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
import org.springframework.web.bind.annotation.RestController;

@Controller
public class ClientController {

    private List<String> dates = new ArrayList<>();
    private List<Double> median = new ArrayList<>();
    private List<Double> variance = new ArrayList<>();
    private List<Double> mean = new ArrayList<>();
    private List<Double> stddev = new ArrayList<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientController.class);
    private static final MessageDeserializer deserializer = new MessageDeserializer(Util.objectMapper);
    private UUID uuid;
    private static final Properties props;
    
    static {
        props = Util.loadPropertiesFromFile("consumer.properties");
        props.setProperty("group.id",  "results_client");
    }

    private final ResultsHandler resultsHandler = new ResultsHandler(props, deserializer);
    private final int partitionId = resultsHandler.getPartitionId();

    @GetMapping("/")
    public String home(Model model) {
        model.addAttribute("Heading", "Home");
        model.addAttribute("counties", County.getCounties());
        return "home.html";
    }

    /**
     * Accepts Post form from Webpage and posts to Balancer to be distributed
     * @param county 
     * @param type
     * @param sDate
     * @param eDate
     * @param minPrice
     * @param maxPrice
     * @param model
     * @return
     * @throws InterruptedException
     */
    @PostMapping("/query")
    public String query(String county, String type, String sDate, String eDate, String minPrice, String maxPrice,
            Model model) throws InterruptedException {
        model.addAttribute("Heading", "Home");
        RestTemplate restTemplate = new RestTemplate();
        Query query = new Query(county, type, "000", sDate, eDate, Double.parseDouble(minPrice),
                Double.parseDouble(maxPrice));
        System.out.println(county + type + sDate + eDate + minPrice + maxPrice);
        uuid = UUID.randomUUID();
        RequestMessage requestMessage = new RequestMessage(uuid, partitionId, query,
                Instant.EPOCH.toEpochMilli());
        HttpEntity<RequestMessage> request = new HttpEntity<>(requestMessage);
        startThread(resultsHandler);

        restTemplate.postForObject("http://192.168.99.101:8081" + "/client", request, RequestMessage.class);
        
        int count = 0;
        int lim = 2;
        while(count != lim) {

            if(resultsHandler.isEmpty(uuid)) {
                LOGGER.info("Empty Results...Polling");
                Thread.sleep(500);
            }
            else {
                LOGGER.info("Results Found");
                StatisticsResult[] results = resultsHandler.getResult(uuid);
                for(StatisticsResult result : results) {
                    System.out.println(result.getStatistics().toString());
                    for(Partition partition : result.getStatistics().keySet()) {
                        if(count == 0) {
                            dates.add(parsePartition(partition));
                            Map<String,Double> temp = result.getStatistics().get(partition);
                            mean.add(temp.get("mean"));
                            median.add(temp.get("median"));
                            variance.add(temp.get("variance"));
                            stddev.add(temp.get("stddev"));
                        } else {
                            //myhomeMap.put(parsePartition(partition), result.getStatistics().get(partition));
                        }
                        count++;
                    }

                }
            }
        }

        model.addAttribute("dates", dates);
        model.addAttribute("mean", mean);
        model.addAttribute("median", median);
        model.addAttribute("variance", variance);
        model.addAttribute("stddev", stddev);
        
        return "display.html";
    }

    private String parsePartition(Partition partition) {
        String result = partition.getValue();
        int datePosition = result.lastIndexOf("_")-5;
        result = result.substring(datePosition+1).replaceAll("_", "-");
        return result;
    }

    private void startThread(ResultsHandler consumer) {
        Thread consumerThread = new Thread(consumer);
        LOGGER.info("Starting Consumer");
        consumerThread.start();
    }
}
