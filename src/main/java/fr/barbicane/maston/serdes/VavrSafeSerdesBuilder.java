package fr.barbicane.maston.serdes;

import static org.apache.kafka.common.serialization.Serdes.serdeFrom;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.barbicane.maston.error.KafkaError;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import io.vavr.control.Validation;
import java.io.Serializable;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;

public class VavrSafeSerdesBuilder {

  public static <T extends SpecificRecord> Serde<Validation<KafkaError<T>, T>> buildForAvro(final Class<T> targetClass,
      final Map<String, ? extends Serializable> configAvroSerdes) {
    final Serde<Validation<KafkaError<T>, T>> serde = serdeFrom(new VavrSafeSerializer<>(new SpecificAvroSerializer<>(), targetClass),
        new VavrSafeDeserializer<>(new SpecificAvroDeserializer<T>(), targetClass));
    serde.configure(configAvroSerdes, false);
    return serde;
  }

  public static <T> Serde<Validation<KafkaError<T>, T>> buildForJson(final Class<T> targetClass) {
    return serdeFrom(new VavrSafeSerializer<>(new JsonSerializer<>(targetClass), targetClass),
        new VavrSafeDeserializer<>(new JsonDeserializer<>(targetClass), targetClass));
  }

  public static <T> Serde<Validation<KafkaError<T>, T>> buildForJson(final Class<T> targetClass, final ObjectMapper objectMapper) {
    return serdeFrom(new VavrSafeSerializer<>(new JsonSerializer<>(objectMapper, targetClass), targetClass),
        new VavrSafeDeserializer<>(new JsonDeserializer<>(objectMapper, targetClass), targetClass));
  }
}
