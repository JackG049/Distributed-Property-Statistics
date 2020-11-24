import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

public class DaftDataSource implements DataSource{
    private DatabaseWrapper databaseWrapper;
    private String databaseName;

    public DaftDataSource(String databaseName, String databaseIp, int databasePort) throws SQLException {
        this.databaseWrapper = new DatabaseWrapper(databaseIp, databasePort);
        this.databaseName = databaseName;

        this.databaseWrapper.addConnection(databaseName);
    }

    @Override
    public List<Map<String, Object>> getHistoricData(LocalDate fromDate, LocalDate untilDate) throws SQLException {
        String firstDateString = dateStringFromMonthBeginning(fromDate);
        String lastDateString = dateStringFromMonthBeginning(fromDate);
        String sqlQuery = String.format("SELECT * FROM historic_data" +
                "WHERE date BETWEEN %s AND %s", firstDateString, lastDateString);

        List<Map<String, Object>> result = databaseWrapper.queryDatabase(sqlQuery, databaseName);
        return result;
    }

    // todo add param checking
    @Override
    public List<Map<String, Object>> getLiveData(LocalDate fromDate, LocalDate untilDate) throws SQLException {
        String firstDateString = dateStringFromMonthBeginning(fromDate);
        String lastDateString = dateStringFromMonthBeginning(fromDate);
        String sqlQuery = String.format("SELECT * FROM historic_data" +
                "WHERE date BETWEEN %s AND %s", firstDateString, lastDateString);

        List<Map<String, Object>> result = databaseWrapper.queryDatabase(sqlQuery, databaseName);
        return result;

    }

    /* My entry for the poorest named method of 2020:
        Takes a date eg 21st March 2020 and transforms it to the first
        day of the month, 1st March 2020, and returns the result as a string
     */
    private String dateStringFromMonthBeginning(LocalDate date) {
        return String.format("%s/%s/01", date.getYear(), date.getMonth());
    }

}
