import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

public interface DataSource {
    public List<Map<String, Object>> getHistoricData(LocalDate fromDate, LocalDate untilDate) throws SQLException;
    public List<Map<String, Object>> getLiveData(LocalDate fromTime, LocalDate untilTime) throws SQLException;
}
