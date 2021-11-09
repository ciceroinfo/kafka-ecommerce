package com.ciceroinfo;

import java.math.BigDecimal;

public class Order {

    private final String email, orderId;
    private final BigDecimal amount;

    public Order(String email, String orderId, BigDecimal amount) {
        this.email = email;
        this.orderId = orderId;
        this.amount = amount;
    }
    
    public String getOrderId() {
        return orderId;
    }
    
    @Override
    public String toString() {
        return "Order{" +
                "email='" + email + '\'' +
                ", orderId='" + orderId + '\'' +
                ", amount=" + amount +
                '}';
    }
}
