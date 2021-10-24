package com.ciceroinfo;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            try (var emailDispatcher = new KafkaDispatcher<String>()) {

                for (int i = 0; i < 10; i++) {

                    String orderId = UUID.randomUUID().toString();
                    var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
                    var email = Math.random() + "@ciceroinfo.com";
                    var order = new Order(email, orderId, amount);

                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);

                    String emailCode = "Thank you for order! We are processing your order!";
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, emailCode);
                }
            }

        }
    }
}
