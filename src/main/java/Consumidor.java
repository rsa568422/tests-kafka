import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumidor {

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.put("group.id", "grupo1");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");

        properties.put("fetch.min.bytes", "1");
        properties.put("fetch.max.wait.ms", "500");
        properties.put("max.partition.fetch.bytes", "1048576");
        properties.put("session.timeout.ms", "10000");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        try {
            consumer.subscribe(Collections.singletonList("topic-test"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Topic: %s, ", record.topic());
                    System.out.printf("Partition: %s, ", record.partition());
                    System.out.printf("Key: %s, ", record.key());
                    System.out.printf("Value: %s.%n", record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
