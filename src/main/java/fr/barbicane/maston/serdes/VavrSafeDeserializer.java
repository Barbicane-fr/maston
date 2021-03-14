package fr.barbicane.maston.serdes;

import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.ERROR_DESERIALIZATION_CODE;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.ERROR_DESERIALIZATION_MESSAGE;
import static io.vavr.control.Validation.invalid;

import fr.barbicane.maston.error.KafkaError;
import io.vavr.control.Option;
import io.vavr.control.Try;
import io.vavr.control.Validation;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.kafka.common.serialization.Deserializer;

@Value
@AllArgsConstructor
public class VavrSafeDeserializer<T> implements Deserializer<Validation<KafkaError<T>, T>> {

  Deserializer<T> deserializer;
  Class<T> targetClass;

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
    this.deserializer.configure(configs, isKey);
  }

  @Override
  public Validation<KafkaError<T>, T> deserialize(final String topic, final byte[] data) {
    return Try.of(() -> deserializer.deserialize(topic, data))
        .fold(throwable -> invalid(KafkaError.<T>builder()
                .targetClass(targetClass)
                .throwable(throwable)
                .code(ERROR_DESERIALIZATION_CODE)
                .message(ERROR_DESERIALIZATION_MESSAGE)
                .bytes(Option.of(data))
                .build()),
            Validation::valid);
  }

  @Override
  public void close() {
    this.deserializer.close();
  }
}
