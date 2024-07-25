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
    public static final String OUTPUT_TOPIC = "json-status-finish";

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
        props.putIfAbsent(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        System.out.println(CustomSerdes.TaskSerde.class);



        //props.putIfAbsent(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        //props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        //props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        //props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CustomSerdes.Task().getClass());


        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    static void createStream(final StreamsBuilder builder) {
        final KStream<String, Task> source = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), CustomSerdes.Task()));


        final KTable<String, Long> counts = source
                .peek((key, value) -> System.out.println(value.id))
                .groupBy((key, value) -> value.id)
                .count();

        // need to override value serde to Long type
        counts.toStream()
                .peek((key, value) -> System.out.println(key + " : " + value))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

/*
        final KStream<String, TaskStatus> out = source
                .peek((key, value) -> System.out.println(value.id))
                .groupBy((key, value) -> key)
                .aggregate(
                        TaskStatus::new,
                        (key, value, agg) -> {
                            agg.id = value.id;
                            agg.state = "GG";
                            agg.tasks.add(value);
                            if (agg.tasks.size() == 2) {
                                agg.state = "OK";
                            }
                            return agg;
                        },
                        //Materialized.with(Serdes.String(), CustomSerdes.TaskStatus())
                        Materialized.as("toto")
                ).toStream()
                .peek((key, value) -> System.out.println(value.id));

        out.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), CustomSerdes.TaskStatus()));


/*
        source.mapValues(value -> {
                    value.state = "Changed";
                    return value;
                })
                .peek((key,value)->  System.out.println(value.id))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), CustomSerdes.Task()));

 */
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
