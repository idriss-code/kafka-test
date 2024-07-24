package json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {

    public static final String CONFIG_VALUE_CLASS = "value.deserializer.class";
    public static final String CONFIG_KEY_CLASS = "key.deserializer.class";
    private final Gson gson = new GsonBuilder().create();

    private Class<T> destinationClass;
    private Type reflectionTypeToken;

    public JsonDeserializer(Class<T> destinationClass) {
        this.destinationClass = destinationClass;
    }

    public JsonDeserializer(Type reflectionTypeToken) {
        this.reflectionTypeToken = reflectionTypeToken;
    }

    public JsonDeserializer() {

    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        if(destinationClass == null && reflectionTypeToken == null){
            String configKey = isKey ? CONFIG_KEY_CLASS : CONFIG_VALUE_CLASS;
            String clsName = String.valueOf(props.get(configKey));

            try {
                destinationClass = (Class<T>) Class.forName(clsName);
            } catch (ClassNotFoundException e) {
                System.err.printf("Failed to configure GsonDeserializer. " +
                                "Did you forget to specify the '%s' property ?%n",
                        configKey);
            }
        }
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        try {
            Type type = destinationClass != null ? destinationClass : reflectionTypeToken;
            return gson.fromJson(new String(bytes, StandardCharsets.UTF_8), type);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing message", e);
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}