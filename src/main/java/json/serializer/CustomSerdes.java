package json.serializer;

import json.Task;
import json.TaskStatus;
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

    public static Serde<TaskStatus> TaskStatus() {
        return new TaskStatusSerde();
    }

    public static class TaskStatusSerde extends Serdes.WrapperSerde<TaskStatus> {
        public TaskStatusSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(TaskStatus.class));
        }
    }
}
