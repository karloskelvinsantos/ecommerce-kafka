package br.com.alura;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class FraudeDetectionService {
    public static void main(String[] args) {
        var fraudeDetectionService = new FraudeDetectionService();
        try(var service = new KafkaService<>(FraudeDetectionService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudeDetectionService::parse,
                Order.class,
                Map.of())){
            service.run();
        }

    }

    public void parse(ConsumerRecord<String, Order> record) {
        System.out.println("---------------------------------");
        System.out.println("Processing new order, for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Order processed");
    }
}
