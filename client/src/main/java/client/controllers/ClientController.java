package client.controllers;

import java.time.Instant;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.mysql.cj.x.protobuf.MysqlxDatatypes.Array;

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
import rest.UrlConstants;
import results.ResultsHandler;
import util.Util;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.web.bind.annotation.PostMapping;

@Controller
public class ClientController {

    private Map<String, Map<String, Double>> daftMap = new HashMap<>();
    private Map<String, Map<String, Double>> myhomeMap = new HashMap<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientController.class);
    private static final MessageDeserializer deserializer = new MessageDeserializer(Util.objectMapper);
    private UUID uuid;
    private static final Properties props;
    private ArrayList<String> dates = new ArrayList<>();
    
    static {
        props = Util.loadPropertiesFromFile("consumer.properties");
        props.setProperty("group.id",  "results_client");
    }

    private final ResultsHandler resultsHandler = new ResultsHandler(props, deserializer);
    private final int partitionId = resultsHandler.getPartitionId();


    @GetMapping("/")
    public String home(Model model) {
        model.addAttribute("Heading", "Home");
        County counties = new County();
        model.addAttribute("counties", counties.getCounties());
        return "home.html";
    }

    @PostMapping("/query")
    public String query(String county, String type, String sDate, String eDate, String minPrice, String maxPrice,
            Model model) throws InterruptedException {
        model.addAttribute("Heading", "Home");
        RestTemplate restTemplate = new RestTemplate();
        Query query = new Query(county, type, "test", sDate, eDate, Double.parseDouble(minPrice),
                Double.parseDouble(maxPrice));
        uuid = UUID.randomUUID();
        RequestMessage requestMessage = new RequestMessage(uuid, partitionId, query,
                Instant.EPOCH.toEpochMilli());
        HttpEntity<RequestMessage> request = new HttpEntity<>(requestMessage);
        restTemplate.postForObject(UrlConstants.BALANCER + "/client", request, RequestMessage.class);
        
        daftMap.clear();
        myhomeMap.clear();

        int count = 0;
        int lim = 2;
        while(count != lim) {

            if(resultsHandler.isEmpty(uuid))
                Thread.sleep(200);
            else {
                StatisticsResult[] results = resultsHandler.getResult(uuid);

                for(StatisticsResult result : results) {
                    for(Partition partition : result.getStatistics().keySet()) {
                        if(count == 0)
                            daftMap.put(parsePartition(partition), result.getStatistics().get(partition));
                        else 
                            myhomeMap.put(parsePartition(partition), result.getStatistics().get(partition));
                    }
                    count++;
                }
            }
        }
        return "display.html";
    }

    private String parsePartition(Partition partition) {
        String res = partition.getValue();
        res = res.split("_")[1];
        return res;
    }
}
