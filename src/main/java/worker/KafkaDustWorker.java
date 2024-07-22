package worker;

import com.dustmobile.backend.service.sim.queue.Exception.AckException;
import com.dustmobile.backend.service.sim.queue.Exception.NoAckException;
import com.dustmobile.backend.service.sim.queue.Worker;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;


public abstract class KafkaDustWorker implements Worker, Runnable {

    private static final Logger LOG = Logger.getLogger(KafkaDustWorker.class.getName());

    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;
    private final String topic;


    protected KafkaDustWorker(String host, String topic) {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":9092");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, topic + "_group");
        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "client-" + UUID.randomUUID());
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.topic = topic;

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    public void init() {
        Thread consumerThread = new Thread(this);
        consumerThread.start();
    }

    public void run() {
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

        try {
            consumer.subscribe(Collections.singletonList(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(30));
                for (ConsumerRecord<String, String> record : records) {
                    LOG.info("Received message: key = " + record.key() + ", value = " + record.value() + ", headers = " + record.headers());


                    String returnTopic = null;

                    Header returnTopicHeader = record.headers().lastHeader("return_topic");
                    if (returnTopicHeader != null) {
                        String headerValue = new String(returnTopicHeader.value());
                        if (!headerValue.equals("NO")) {
                            returnTopic = headerValue;
                        }
                    }

                    String uuid = null;
                    Header uuidHeader = record.headers().lastHeader("correlation_id");
                    if (uuidHeader != null) {
                        uuid = new String(uuidHeader.value());
                    }

                    String returnMessage = null;

                    try {
                        returnMessage = process(record.value());
                    } catch (AckException e) {
                        LOG.severe(e.getMessage());
                        returnMessage = e.getMessage();
                    } catch (Exception e) { // NoAckException include
                        LOG.severe("error return message to queue" + e);
                        return;
                    }

                    if (returnMessage != null && returnTopic != null) {
                        LOG.info("return '" + returnTopic + " : " + returnMessage);

                        ProducerRecord<String, String> responseRecord = new ProducerRecord<>(returnTopic, null, returnMessage);
                        if (uuid != null) {
                            responseRecord.headers().add("correlation_id", uuid.getBytes());
                        }

                        producer.send(responseRecord).get();
                    }

                    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                    OffsetAndMetadata offset = new OffsetAndMetadata(record.offset() + 1); // Commit the next offset
                    currentOffsets.put(partition, offset);
                    consumer.commitAsync(currentOffsets, new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                            if (exception != null) {
                                // Handle commit exception
                                LOG.severe("Commit failed for offsets " + offsets + " - "+ exception);
                            } else {
                                LOG.finest("Commit succeeded for offsets " + offsets);
                            }
                        }
                    });
                }
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            consumer.close();
        }
    }

    public abstract String process(String task) throws AckException, NoAckException;
}
