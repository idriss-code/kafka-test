package worker;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

public class KafkaDustProducer implements Producer {

    private static final Logger LOG = Logger.getLogger(KafkaDustProducer.class.getName());
    private final String topic;
    private final String returnTopic;

    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private static final String GROUP_ID = "rpc-group";
    public KafkaDustProducer(String host, String topic) {
        this(host, topic, null);
    }

    public KafkaDustProducer(String host, String topic, String returnTopic) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
        this.topic = topic;


            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":9092");
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumer = new KafkaConsumer<>(consumerProps);


        this.returnTopic = returnTopic;

    }

    public void send(String message) throws Exception {

        String key = null;

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);

        Headers headers = record.headers();
        byte[] bytes = returnTopic != null ? returnTopic.getBytes() : "NO".getBytes();
        headers.add(new RecordHeader("return_topic", bytes));

        RecordMetadata metadata = producer.send(record).get();

        LOG.info("Sent message to topic: " + metadata.topic() + ", partition: " + metadata.partition() + ", offset: " + metadata.offset());
    }

    public String syncSend(String message) throws Exception {

        String key = null;
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
        final String correlationId = UUID.randomUUID().toString();
        String responseTopic = "rpc-replies-" + correlationId;

        Headers headers = record.headers();
        headers.add(new RecordHeader("correlation_id", correlationId.getBytes()));
        headers.add(new RecordHeader("return_topic", responseTopic.getBytes()));

        RecordMetadata metadata = producer.send(record).get();

        LOG.info("Sent message to topic: " + metadata.topic() + ", partition: " + metadata.partition() + ", offset: " + metadata.offset());
        consumer.subscribe(Collections.singletonList(responseTopic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> consumerRecord : records) {
                String receivedCorrelationId = new String(consumerRecord.headers().lastHeader("correlation_id").value());
                if (correlationId.equals(receivedCorrelationId)) {
                    consumer.close();
                    return consumerRecord.value();
                }
            }
        }
    }
}
