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
            packageData();
        }

        sendData();
    }

    private void getLatestDatabaseEntry(String tableName) {

    }

    public ItemCollection<QueryOutcome> pullFromDatabase(String tableName, String periodStart, String periodEnd, Map<String, Object> attributes) {
        //return databaseWrapper.queryTable(tableName, periodStart, periodEnd);//, attributes);
        return null;
    }

    private void packageData() {

    }


    private void pullRecentDataFromSource(String tableName) {
        // pull from daft & myhomes

        // store data

    }

    private void sendData() {

    }
}
