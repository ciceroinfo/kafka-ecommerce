package com.ciceroinfo;

import com.ciceroinfo.consumer.ConsumerService;
import com.ciceroinfo.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.UUID;

public class CreateUserService implements ConsumerService<Order> {
    
    private final LocalDatabase database;
    
    public CreateUserService() throws SQLException {
        this.database = new LocalDatabase("users_database");
        this.database.createIfNotExists("create table Users (" +
                "uuid varchar(200) primary key, " +
                "email varchar(200))");
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
        var uuid = UUID.randomUUID().toString();
        var success = database.update("insert into Users(uuid, email) values (?, ?)", uuid, email);
        
        if (success) {
            System.out.println("User " + uuid + " e " + email + " added");
        } else {
            System.out.println("User " + uuid + " e " + email + " NOT added");
        }
    }
    
    private boolean isNewUser(String email) throws SQLException {
        var results = database.query("select uuid from Users where email = ? limit 1", email);
        return !results.next();
    }
}
