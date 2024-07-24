package json;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class Consumer {
    public static void main(String[] args) {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "json-client-1");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());

        consumerProperties.put(JsonDeserializer.CONFIG_VALUE_CLASS, Task.class.getName());

        KafkaConsumer<String, Task> consumer = new KafkaConsumer<>(consumerProperties);

        consumer.subscribe(Arrays.asList("json-output"));

        while (true) {
            ConsumerRecords<String, Task> records = consumer.poll(Duration.ofSeconds(10));
            //System.out.println("records: " + records.count());
            for (ConsumerRecord<String, Task> record : records) {
                System.out.println(record.value().id);
                System.out.println(record.value().state);
            }
        }
    }
}