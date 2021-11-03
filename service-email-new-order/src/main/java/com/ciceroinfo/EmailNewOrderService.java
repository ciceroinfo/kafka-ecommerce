package com.ciceroinfo;

import com.ciceroinfo.consumer.KafkaService;
import com.ciceroinfo.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class EmailNewOrderService<T> {
    
    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
    private KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();
    
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var emailService = new EmailNewOrderService();
        try (var service = new KafkaService<>(EmailNewOrderService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                emailService::parse,
                Map.of())) {
            service.run();
        }
    }
    
    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
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
