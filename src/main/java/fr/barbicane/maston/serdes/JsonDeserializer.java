package fr.barbicane.maston.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.kafka.common.serialization.Deserializer;

@Value
public class JsonDeserializer<T> implements Deserializer<T> {

  ObjectMapper objectMapper;
  Class<T> targetType;

  public JsonDeserializer(final Class<T> targetType) {
    this.objectMapper = new ObjectMapper();
    this.targetType = targetType;
  }

  // To pass custom object mapper
  public JsonDeserializer(final ObjectMapper objectMapper, final Class<T> targetType) {
    this.objectMapper = objectMapper;
    this.targetType = targetType;
  }


  @SneakyThrows
  @Override
  public T deserialize(String s, byte[] bytes) {
    return objectMapper.readValue(bytes, targetType);
  }

}
