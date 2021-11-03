package com.ciceroinfo;

import com.ciceroinfo.consumer.ConsumerService;
import com.ciceroinfo.consumer.KafkaService;
import com.ciceroinfo.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ReadingReportService<T> implements ConsumerService<User> {


    private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new ServiceRunner(ReadingReportService::new).start(5);
    }
    @Override
    public void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
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
    
    @Override
    public String getTopic() {
        return "ECOMMERCE_USER_GENERATE_READING_REPORT";
    }
    
    @Override
    public String getConsumerGroup() {
        return ReadingReportService.class.getSimpleName();
    }
}
