package com.ciceroinfo;

import com.ciceroinfo.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class EmailService implements ConsumerService<String> {
    
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new ServiceRunner(EmailService::new).start(5);
    }
    
    public String getTopic() {
        return "ECOMMERCE_SEND_EMAIL";
    }
    
    public String getConsumerGroup() {
        return EmailService.class.getSimpleName();
    }
    
    public void parse(ConsumerRecord<String, Message<String>> record) {
        System.out.println("----------------------------");
        System.out.println("topic[" + record.topic() + "] ::: partition[" + record.partition() + "] ::: offset:[" + record.offset() + "] ::: timestamp:[" + record.timestamp() + "]");
        System.out.println("Sending EMAIL: KEY[" + record.key() + "] ::: VALUE[" + record.value() + "]");
        System.out.println("----------------------------");
    }
}
