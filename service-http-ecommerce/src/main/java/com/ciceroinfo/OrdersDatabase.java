package com.ciceroinfo;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;

public class OrdersDatabase implements Closeable {
    
    private final LocalDatabase database;
    
    public OrdersDatabase() throws SQLException {
        this.database = new LocalDatabase("orders_database");
        // you might want to save all data
        this.database.createIfNotExists("create table IF NOT EXISTS Orders (" +
                "uuid varchar(200) primary key)");
    }
    
    public boolean saveNew(Order order) throws SQLException {
        if (wasProcessed(order)) {
            System.out.println("Order " + order.getOrderId() + " was already processed");
            return false;
        }
        database.update("insert into Orders (uuid) values (?)", order.getOrderId());
        return true;
    }
    
    private boolean wasProcessed(Order order) throws SQLException {
        var result = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
        return result.next();
    }
    
    @Override
    public void close() throws IOException {
        database.close();
    }
}
