package br.com.alura;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var properties = properties();
        var producer = new KafkaProducer<String, String>(properties);
        var value = "123123,2432,54545";
        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);
        producer.send(record, (data, error) -> {
            if (error != null) {
                error.printStackTrace();
                return;
            }

            System.out.println("Mensagem enviada, topico:::" + data.topic() + "/ offset" + data.offset() + "/ partition " + data.partition() + "/ time " + data.timestamp());
        }).get();
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
