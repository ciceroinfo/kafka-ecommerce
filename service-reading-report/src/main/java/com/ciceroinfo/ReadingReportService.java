package com.ciceroinfo;

import com.ciceroinfo.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ReadingReportService<T> {


    private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var readingReportService = new ReadingReportService();
        try (var service = new KafkaService<>(ReadingReportService.class.getSimpleName(),
                "ECOMMERCE_USER_GENERATE_READING_REPORT",
                readingReportService::parse,
                Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<User>> record) throws ExecutionException, InterruptedException,
            IOException {
        System.out.println("----------------------------");
        System.out.println("topic[" + record.topic() + "] ::: partition[" + record.partition() + "] ::: offset:[" + record.offset() + "] ::: timestamp:[" + record.timestamp() + "]");
        System.out.println("Processing REPORT: KEY[" + record.key() + "] ::: VALUE[" + record.value() + "]");

        var message = record.value();
        var user = message.getPayload();

        var target = new File(user.getReportPath());
        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for " + user.getUuid());


        System.out.println("File created: " + target.getAbsolutePath());
        System.out.println("Report processed");
        System.out.println("----------------------------");

    }
}
