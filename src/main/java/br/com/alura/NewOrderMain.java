package br.com.alura;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        for (int i = 0; i < 10; i++) {
            var key = UUID.randomUUID().toString();
            var value = key + ",2432,54545";

            try(var dispatcher = new KafkaDispatcher()) {
                dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

                var email = "Thank you for your shop! We are processing your order!";
                dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
            }
        }
    }


}
