package com.ciceroinfo;

import com.ciceroinfo.consumer.ConsumerService;
import com.ciceroinfo.consumer.KafkaService;
import com.ciceroinfo.consumer.ServiceRunner;
import com.ciceroinfo.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class EmailNewOrderService<T> implements ConsumerService<Order> {
    
    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
    private KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();
    
    public static void main(String[] args) {
        new ServiceRunner(EmailNewOrderService::new).start(1);
    }
    
    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }
    
    @Override
    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }
    
    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("----------------------------");
        System.out.println("topic[" + record.topic() + "] ::: partition[" + record.partition() + "] ::: offset:[" + record.offset() + "] ::: timestamp:[" + record.timestamp() + "]");
        System.out.println("Processing ORDER EMAIL: KEY[" + record.key() + "] ::: VALUE[" + record.value() + "]");
        
        var message = record.value();
        var order = message.getPayload();
        var id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());
        
        String emailCode = "Thank you for order! We are processing your order!";
        emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(),
                id,
                emailCode);
        
        System.out.println("Email processed");
        System.out.println("----------------------------");
        
        sleep();
    }
    
    private static void sleep() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }
    }
}
