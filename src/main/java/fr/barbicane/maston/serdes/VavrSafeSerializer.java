package fr.barbicane.maston.serdes;

import fr.barbicane.maston.error.KafkaError;
import io.vavr.control.Try;
import io.vavr.control.Validation;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

@Value
@AllArgsConstructor
@Slf4j
public class VavrSafeSerializer<T> implements Serializer<Validation<KafkaError<T>, T>> {

  Serializer<T> serializer;
  Class<T> targetClass;

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
    this.serializer.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, Validation<KafkaError<T>, T> tValidation) {
    if (tValidation.isValid()) {
      return serializeT(topic, tValidation.get());
    } else {
      final KafkaError<T> error = tValidation.getError();
      return error.getBytes().getOrElse(
          () -> error.getSourceRecord().map(t -> serializeT(topic, t))
              .getOrElse(() -> {
                LOGGER.error("Error while serializing class {} to topic {} : no data were found in safe serializer entry method.",
                    targetClass.getCanonicalName(), topic);
                // Return 0 bytes to ensure to push header into error topic to get initial error metadata.
                return new byte[0];
              }));
    }
  }


  private byte[] serializeT(String topic, T t) {
    return Try.of(() -> serializer.serialize(topic, t))
        .fold(throwable -> {
              LOGGER.error("Error while serializing {} to class {} to topic {} : {}", t.toString(),
                  targetClass.getCanonicalName(), topic, throwable);
              // Return 0 bytes to ensure to push header into error topic to get initial error metadata.
              return new byte[0];
            },
            bytes -> bytes);
  }

  @Override
  public void close() {
    this.serializer.close();
  }
}
