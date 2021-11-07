package com.ciceroinfo;

import com.ciceroinfo.consumer.ConsumerService;
import com.ciceroinfo.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;

public class CreateUserService implements ConsumerService<Order> {
    
    private final Connection connection;
    
    private CreateUserService() throws SQLException {
        String url = "jdbc:sqlite:service-users/target/users_database.db";
        this.connection = DriverManager.getConnection(url);
        
        try {
            connection.createStatement().execute("create table Users (" +
                    "uuid varchar(200) primary key, " +
                    "email varchar(200))");
        } catch (SQLException e) {
            // be careful, the SQL could be wrong, be reallly careful
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        new ServiceRunner<>(CreateUserService::new).start(1);
    }
    
    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }
    
    @Override
    public String getConsumerGroup() {
        return CreateUserService.class.getSimpleName();
    }
    
    public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
        System.out.println("----------------------------");
        System.out.println("topic[" + record.topic() + "] ::: partition[" + record.partition() + "] ::: offset:[" + record.offset() + "] ::: timestamp:[" + record.timestamp() + "]");
        System.out.println("Processing USER: KEY[" + record.key() + "] ::: VALUE[" + record.value() + "]");
        
        var message = record.value();
        var order = message.getPayload();
        
        if (isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        } else {
            System.out.println("User " + order.getEmail() + " already exists");
        }
        
        System.out.println("User processed");
        System.out.println("----------------------------");
        
        
    }
    
    
    private void insertNewUser(String email) throws SQLException {
        var insert = connection.prepareStatement("insert into Users(uuid, email) values (?, ?)");
        var uuid = UUID.randomUUID().toString();
        insert.setString(1, uuid);
        insert.setString(2, email);
        insert.execute();
        System.out.println("User " + uuid + " e " + email + " added");
    }
    
    private boolean isNewUser(String email) throws SQLException {
        var exists = connection.prepareStatement("select uuid from Users where email = ? limit 1");
        exists.setString(1, email);
        var results = exists.executeQuery();
        return !results.next();
    }
}
