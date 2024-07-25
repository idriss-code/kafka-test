package stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Properties;

public class KTableReader {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-reader-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class);

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, Long> firstKTable = builder.globalTable("streams-wordcount-output",
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("my-store-name")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();

        // Get the store
        ReadOnlyKeyValueStore<String, Long> keyValueStore =
                streams.store(StoreQueryParameters.fromNameAndType("my-store-name", QueryableStoreTypes.keyValueStore()));

        // Query the store
        String key = "world";
        Long value = keyValueStore.get(key);
        System.out.println("Value for '" + key + "': " + value);

        KeyValueIterator<String, Long> it = keyValueStore.all();
        while (it.hasNext()) {
            System.out.println(it.next());
        }

        streams.close();
    }
}
