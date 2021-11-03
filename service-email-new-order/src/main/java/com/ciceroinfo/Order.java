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

    public String getEmail() {
        return email;
    }

    public BigDecimal getAmount() {
        return amount;
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
