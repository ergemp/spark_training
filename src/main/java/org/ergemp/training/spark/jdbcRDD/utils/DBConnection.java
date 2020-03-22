package org.ergemp.training.spark.jdbcRDD.utils;

import scala.runtime.AbstractFunction0;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DBConnection extends AbstractFunction0<Connection> implements Serializable {

    private String driverClassName;
    private String connectionUrl;
    private String userName;
    private String password;

    public DBConnection(String driverClassName, String connectionUrl, String userName, String password) {
        this.driverClassName = driverClassName;
        this.connectionUrl = connectionUrl;
        this.userName = userName;
        this.password = password;
    }

    public DBConnection() {
        this.driverClassName = "org.postgresql.Driver";
        this.connectionUrl = "jdbc:postgresql://localhost/postgres";
        this.userName = "postgres";
        this.password = "";
    }

    @Override
    public Connection apply() {
        try {
            Class.forName(driverClassName);
        } catch (ClassNotFoundException e) {
            //LOGGER.error("Failed to load driver class", e);
            e.printStackTrace();
        }

        Properties properties = new Properties();
        properties.setProperty("user", userName);
        properties.setProperty("password", password);

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(connectionUrl, properties);
        } catch (SQLException e) {
            //LOGGER.error("Connection failed", e);
            e.printStackTrace();
        }
        return connection;
    }
}
