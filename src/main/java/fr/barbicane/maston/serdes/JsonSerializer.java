package fr.barbicane.maston.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serializer;


@AllArgsConstructor
public class JsonSerializer<T> implements Serializer<T> {

  ObjectMapper objectMapper;
  Class<T> targetClass;

  public JsonSerializer(final Class<T> targetClass) {
    this.objectMapper = new ObjectMapper();
    this.targetClass = targetClass;
  }

  // To pass custom objectMapper
  public JsonSerializer(final Class<T> targetClass, final ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
    this.targetClass = targetClass;
  }

  @SneakyThrows
  @Override
  public byte[] serialize(String s, T t) {
    return objectMapper.writeValueAsBytes(t);
  }

}
