package com.ciceroinfo;

import com.ciceroinfo.dispatcher.KafkaDispatcher;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        
        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            for (int i = 0; i < 10; i++) {
                
                String orderId = UUID.randomUUID().toString();
                var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
                var email = Math.random() + "@ciceroinfo.com";
                var order = new Order(email, orderId, amount);
                
                orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, new CorrelationId(NewOrderMain.class.getSimpleName()), order);
            }
        }
    }
}
