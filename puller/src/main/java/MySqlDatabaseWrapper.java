import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;

/**
 * A Java MySQL SELECT statement example.
 * Demonstrates the use of a SQL SELECT statement against a
 * MySQL database, called from a Java program.
 *
 * Created by Alvin Alexander, http://alvinalexander.com
 */
public class MySqlDatabaseWrapper {
    // Maps a database name e.g 'daft_ie' to a db connection
    private final Map<String, Connection> connectionMap = new HashMap();
    private String databaseIp;
    private int databasePortNum;

    // todo add param checking
    public MySqlDatabaseWrapper(String databaseIp, int databasePortNum){
        this.databaseIp = databaseIp;
        this.databasePortNum = databasePortNum;
    }

    public void addConnection(String databaseName) throws SQLException {
        if (StringUtils.isEmpty(databaseName)) {
            throw new IllegalArgumentException("Database name cannot be empty");
        }

        String databaseUrl = String.format("jdbc:mysql://%s:%s/%s", databaseIp, databasePortNum, databaseName);
        Connection connection = DriverManager.getConnection(databaseUrl, "root", "password");
        connectionMap.put(databaseName, connection);
    }

    public void closeConnection(String databaseName) throws SQLException {
        Connection connection = connectionMap.get(databaseName);
        if (connection != null) {
            connection.close();
        }
    }

    public void closeAllConnections() throws SQLException {
        AtomicBoolean successfulClosure = new AtomicBoolean(true);
        connectionMap.values().forEach(connection -> {
           try {
               connection.close();
           } catch (SQLException throwables) {
               throwables.printStackTrace();
               successfulClosure.set(false);
           }
        });

        if (!successfulClosure.get()) {
            throw new SQLException("Failed to close all connections");
        }

    }

    public List<Map<String, Object>> queryDatabase(String sqlQuery, String databaseName) throws SQLException {
        if (StringUtils.isEmpty(databaseName)) {
            throw new IllegalArgumentException("Database name cannot be empty");
        }
        if (StringUtils.isEmpty(sqlQuery)) {
            throw new IllegalArgumentException("SQL query name cannot be empty");
        }

        Connection databaseConnection = connectionMap.get(databaseName);
        if (databaseConnection == null) {
            throw new IllegalArgumentException("Database connection doesn't exist");
        }

        MapListHandler beanListHandler = new MapListHandler();
        QueryRunner runner = new QueryRunner();
        List<Map<String, Object>> list = runner.query(databaseConnection, sqlQuery, beanListHandler);

        return list;
    }


}