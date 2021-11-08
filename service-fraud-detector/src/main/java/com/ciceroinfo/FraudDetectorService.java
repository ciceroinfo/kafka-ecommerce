package com.ciceroinfo;

import com.ciceroinfo.consumer.ConsumerService;
import com.ciceroinfo.consumer.ServiceRunner;
import com.ciceroinfo.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService implements ConsumerService<Order> {
    
    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
    private final LocalDatabase database;
    
    public FraudDetectorService() throws SQLException {
        this.database = new LocalDatabase("frauds_database");
        this.database.createIfNotExists("create table Orders (" +
                "uuid varchar(200) primary key, " +
                "is_fraud boolean)");
    }
    
    public static void main(String[] args)  {
        new ServiceRunner<>(FraudDetectorService::new).start(1);
    }
    
    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }
    
    @Override
    public String getConsumerGroup() {
        return FraudDetectorService.class.getSimpleName();
    }
    
    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("----------------------------");
        System.out.println("topic[" + record.topic() + "] ::: partition[" + record.partition() + "] ::: offset:[" + record.offset() + "] ::: timestamp:[" + record.timestamp() + "]");
        System.out.println("Processing ORDER: KEY[" + record.key() + "] ::: VALUE[" + record.value() + "]");
        
        var message = record.value();
        var order = message.getPayload();
        
        if (wasProcessed(order)) {
            System.out.println("Order " + order.getOrderId() + " was already processed");
            return;
        }
        
        if (isFraud(order)) {
            System.out.println("Order is a fraud!");
            database.update("insert into Orders (uuid, is_fraud) values (?, true)", order.getOrderId());
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(),
                    message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        } else {
            System.out.println("Approved");
            database.update("insert into Orders (uuid, is_fraud) values (?, false)", order.getOrderId());
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        }
        
        System.out.println("Order processed");
        System.out.println("----------------------------");
        
        sleep(5000);
    }
    
    private boolean wasProcessed(Order order) throws SQLException {
        var result = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
        return result.next();
    }
    
    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }
    }
    
    /**
     * pretending that the fraud happens when the amount is >= 4500
     */
    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal(4500)) >= 0;
    }
}
