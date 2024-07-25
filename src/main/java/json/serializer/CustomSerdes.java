package json.serializer;

import json.Task;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public final class CustomSerdes {
    private CustomSerdes() {
    }

    public static Serde<Task> Task() {
        return new TaskSerde();
    }

    public static class TaskSerde extends Serdes.WrapperSerde<Task> {
        public TaskSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(Task.class));
        }
    }
}
