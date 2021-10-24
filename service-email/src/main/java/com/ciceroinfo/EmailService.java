package com.ciceroinfo;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class EmailService {

    public static void main(String[] args) {
        var emailService = new EmailService();
        try (var service = new KafkaService(EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse,
                String.class,
                Map.of())) {
            service.run();
        }

        sleep(1000);
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("----------------------------");
        System.out.println("topic[" + record.topic() + "] ::: partition[" + record.partition() + "] ::: offset:[" + record.offset() + "] ::: timestamp:[" + record.timestamp() + "]");
        System.out.println("Sending EMAIL: KEY[" + record.key() + "] ::: VALUE[" + record.value() + "]");
        System.out.println("----------------------------");
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }
    }
}
