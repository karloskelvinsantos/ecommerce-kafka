package br.com.alura;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var orderDispatcher = new KafkaDispatcher<Order>()) {
            try(var emailDispacher = new KafkaDispatcher<Email>()) {
                var email = Math.random() + "@email.com";
                for (int i = 0; i < 10; i++) {
                    var orderId = UUID.randomUUID().toString();
                    var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);

                    var order = new Order(orderId, amount, email);
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);

                    var emailCode = new Email(email, "Thank you for your shop! We are processing your order!");
                    emailDispacher.send("ECOMMERCE_SEND_EMAIL", email, emailCode);
                }
            }
        }
    }
}
