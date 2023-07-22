package org.wl.models;

import java.sql.Connection;
import java.sql.*;

public class DBConnection {

    private static Connection connection = null;
    private DBConnection(Connection connection) {
        this.connection = connection;
    }

    public static Connection getInstance() {

        if (connection == null) {
            connection = createConnection();
        }
        return connection;
    }

    private static Connection createConnection() {
        Connection conn = null;
        try {
            Class.forName("org.h2.Driver");
            conn = DriverManager.getConnection("jdbc:h2:~/test", "mohdmsl", "qwerty_100");
            initializeDatabase(conn);
            initializeTables(conn);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    private static void initializeDatabase(Connection conn) {
        // SQL command to create a database in MySQL.
        String sql = "CREATE SCHEMA IF NOT EXISTS test";
        try {
            PreparedStatement stmt = conn.prepareStatement(sql);

            stmt.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void initializeTables(Connection conn) {
        String sqlCreate = "CREATE TABLE IF NOT EXISTS CONNECTION"
       + "(ID VARCHAR(50),"
                + " name VARCHAR(100),"
                + " zookeeperHost VARCHAR(200),"
                + " zookeeperPort VARCHAR(50) ,"
                + " bootstrapServer VARCHAR(500),"
                + " keystoreLocation VARCHAR(500),"
                + " keyStorePassword VARCHAR(500),"
                + " truststoreLocation VARCHAR(500), "
                + " truststorePassword VARCHAR(500),"
                + " mechanism VARCHAR(20),"
                + " keyPassword VARCHAR(500),"
                + " identificationAlgorithm BOOLEAN)";

        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.executeUpdate(sqlCreate);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
