package com.ciceroinfo;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) {

        var logService = new LogService();

        try (var service = new KafkaService(EmailService.class.getSimpleName(), Pattern.compile("ECOMMERCE.*"), logService::parse)) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("----------------------------");
        System.out.println("topic[" + record.topic() + "] ::: partition[" + record.partition() + "] ::: offset:[" + record.offset() + "] ::: timestamp:[" + record.timestamp() + "]");
        System.out.println("LOG: KEY[" + record.key() + "] ::: VALUE[" + record.value() + "]");
        System.out.println("----------------------------");
    }
}
