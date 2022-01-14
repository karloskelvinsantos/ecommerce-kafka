package br.com.alura;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudeDetectionService {
    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

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

    public void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("---------------------------------");
        System.out.println("Processing new order, for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        var order = record.value();
        if (isFraude(order)) {
            //making a order with fraud, when amount >= 4500
            System.out.println("Order is a fraud! " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order);
        } else {
            System.out.println("Approved " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_ACCEPT", order.getEmail(), order);
        }
    }

    private boolean isFraude(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
