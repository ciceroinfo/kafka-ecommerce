package com.ciceroinfo;

import java.sql.*;

public class LocalDatabase {
    
    private final Connection connection;
    
    LocalDatabase(String name) throws SQLException {
        String url = "jdbc:sqlite:common-database/target/" + name + ".db";
        this.connection = DriverManager.getConnection(url);
    }
    
    // yes, this is way too generic
    // according to your database tool, avoid injection
    public void createIfNotExists(String sql) {
        try {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            // be careful, the SQL could be wrong, be really careful
            e.printStackTrace();
        }
    }
    
    public boolean update(String statement, String... params) throws SQLException {
        return prepare(statement, params).execute();
    }
    
    public ResultSet query(String query, String... params) throws SQLException {
        return prepare(query, params).executeQuery();
    }
    
    private PreparedStatement prepare(String query, String[] params) throws SQLException {
        var preparedStatement = connection.prepareStatement(query);
        for (int i = 0; i < params.length; i++) {
            preparedStatement.setString(i + 1, params[i]);
        }
        return preparedStatement;
    }
}
