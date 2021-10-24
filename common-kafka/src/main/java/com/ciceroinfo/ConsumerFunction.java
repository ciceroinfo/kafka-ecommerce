package com.ciceroinfo;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction<T> {
    void consume(ConsumerRecord<String, T> record) throws Exception;
}
