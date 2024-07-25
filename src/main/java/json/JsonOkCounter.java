package json;

import json.serializer.CustomSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class JsonOkCounter {

    public static final String INPUT_TOPIC = "json-status_return";
    public static final String OUTPUT_TOPIC = "json-status";

    static Properties getStreamsConfig(final String[] args) throws IOException {
        final Properties props = new Properties();
        if (args != null && args.length > 0) {
            try (final FileInputStream fis = new FileInputStream(args[0])) {
                props.load(fis);
            }
            if (args.length > 1) {
                System.out.println("Warning: Some command line arguments were ignored. This demo only accepts an optional configuration file.");
            }
        }
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "streams-"+ UUID.randomUUID().toString());
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //props.putIfAbsent(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);  // emit after each input
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    static void createStream(final StreamsBuilder builder) {
        final KStream<String, Task> source = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), CustomSerdes.Task()));

        final KStream<String, TaskStatus> out = source
                .peek((key, value) -> System.out.println(value.id + value.worker))
                //.filter((key,value)->value.state.equals("OK"))
                .groupBy((key, value) -> value.id)
                .aggregate(
                        TaskStatus::new,
                        (key, value, agg) -> {
                            agg.id = value.id;
                            agg.state = "PENDING";
                            agg.tasks.add(value);
                            if (agg.tasks.size() >= 2) {
                                agg.state = "OK";
                            }
                            return agg;
                        },
                        Materialized.with(Serdes.String(), CustomSerdes.TaskStatus())
                ).toStream()
                .peek((key, value) -> System.out.println(value.id + "out"));

        out.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), CustomSerdes.TaskStatus()));
    }

    public static void main(final String[] args) throws IOException {
        final Properties props = getStreamsConfig(args);

        final StreamsBuilder builder = new StreamsBuilder();
        createStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-json-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            System.out.println("starting");
            streams.cleanUp();
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
