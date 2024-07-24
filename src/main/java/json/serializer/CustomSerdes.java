package json.serializer;

import json.Task;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public final class CustomSerdes {
    private CustomSerdes() {
    }

    public static Serde<Task> Task() {
        JsonSerializer<Task> serializer = new JsonSerializer<>();
        JsonDeserializer<Task> deserializer = new JsonDeserializer<>(Task.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
