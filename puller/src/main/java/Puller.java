import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;

import java.util.Map;
import java.util.Set;

public class Puller {
    private final PropertyDbWrapper databaseWrapper = new PropertyDbWrapper();
    private Set<String> tableNames;

    public Puller() {
        tableNames = databaseWrapper.getTableNames();
    }

    public void query(Map<String, Object> query) {
        for (String tableName : tableNames) {
            getLatestDatabaseEntry(tableName);
            pullRecentDataFromSource(tableName);
            updateDatabase(tableName);
            packageData();
        }

        sendData();
    }

    /**
    Get the date of the latest update to the database
     */
    private void getLatestDatabaseEntry(String tableName) {

    }

    /**
     * Retrieve all relevent data from the database
     * @param tableName
     * @param periodStart
     * @param periodEnd
     * @param attributes
     * @return
     */
    public ItemCollection<QueryOutcome> pullFromDatabase(String tableName, String periodStart, String periodEnd, Map<String, Object> attributes) {
        //return databaseWrapper.queryTable(tableName, periodStart, periodEnd);//, attributes);
        return null;
    }

    /**
     * Bundle up data that needs to be processed
     */
    private void packageData() {

    }

    /**
     * Check data sources for new data and pull it into
     * @param tableName
     */
    private void pullRecentDataFromSource(String tableName) {
        // pull from daft & myhomes

        // store data
    }

    /**
     * Update database with new data
     */
    private void updateDatabase(String tableName) {

    }

    /**
     * Send data to be processed
     */
    private void sendData() {

    }
}
