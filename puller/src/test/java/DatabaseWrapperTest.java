import com.google.common.base.Joiner;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.Assume.assumeTrue;

public class DatabaseWrapperTest {
    private final static DatabaseWrapper databaseWrapper = new DatabaseWrapper();
    private final static String databaseName = "daft_ie";
    private static boolean isDatabaseRunning = false;

    @BeforeClass
    public static void setup() throws SQLException {
        try {
            databaseWrapper.addConnection(databaseName);
            isDatabaseRunning = true;
        } catch (CommunicationsException e) {
            System.out.println("The database is not running. Aborting further tests.");
        }
    }

    @Test
    public void canMakeSqlQuery() throws SQLException {
        assumeTrue(isDatabaseRunning);

        String query = "SELECT * FROM historic_data";

        databaseWrapper.addConnection(databaseName);
        List<Map<String, Object>> results = databaseWrapper.queryDatabase(databaseName, query);

        if (!results.isEmpty()) {
            System.out.println("First entry: " + Joiner.on(",").withKeyValueSeparator("=").join(results.get(0)));
        }
    }
}
