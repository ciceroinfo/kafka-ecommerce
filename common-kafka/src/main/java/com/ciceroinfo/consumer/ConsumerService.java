package com.ciceroinfo.consumer;

import com.ciceroinfo.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerService<T> {
    
    // you may argue that a ConsumerException would be better
    // and this ok, it can be better
    void parse(ConsumerRecord<String, Message<T>> record) throws Exception;
    String getTopic();
    String getConsumerGroup();
}
