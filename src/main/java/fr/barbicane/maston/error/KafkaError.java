package fr.barbicane.maston.error;

import static io.vavr.control.Option.none;

import io.vavr.control.Option;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

@Builder
@Data
@ToString
public class KafkaError<T> {

  private String message;
  private String code;
  private Throwable throwable;
  private Class<T> targetClass;
  @Builder.Default
  private Option<byte[]> bytes = none();
  @Builder.Default
  private Option<T> sourceRecord = none();

}
