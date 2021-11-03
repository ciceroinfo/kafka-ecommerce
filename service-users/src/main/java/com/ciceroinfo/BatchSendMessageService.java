package com.ciceroinfo;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {
    
    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();
    private final Connection connection;
    
    public BatchSendMessageService() throws SQLException {
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
    
    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
        var batchService = new BatchSendMessageService();
        try (var service = new KafkaService<>(BatchSendMessageService.class.getSimpleName(),
                "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                batchService::parse,
                Map.of())) {
            service.run();
        }
    }
    
    private void parse(ConsumerRecord<String, Message<String>> record) throws ExecutionException, InterruptedException,
            SQLException {
        System.out.println("----------------------------Async Mode");
        var message = record.value();
        System.out.println("topic[" + record.topic() + "] ::: partition[" + record.partition() + "] ::: offset:[" + record.offset() + "] ::: timestamp:[" + record.timestamp() + "]");
        System.out.println("Processing BATCH: KEY[" + record.key() + "] ::: VALUE[" + record.value() + "]");
        
        if (Math.random() < 0.5) throw new RuntimeException("deu erro que eu forcei");
        
        for (User user : getAllUsers()) {
            userDispatcher.sendAsync(message.getPayload(), user.getUuid(),
                    message.getId().continueWith(BatchSendMessageService.class.getSimpleName()),
                user);
            System.out.println("Acho que enviei para " + user);
        }
        
        System.out.println("batch processed");
        System.out.println("----------------------------");
        
        var order = record.value();
        
        
    }
    
    private List<User> getAllUsers() throws SQLException {
        
        var results = connection.prepareStatement("select uuid from Users").executeQuery();
        
        List<User> users = new ArrayList<User>();
        
        while (results.next()) {
            users.add(new User(results.getString(1)));
        }
        
        return users;
    }
}
