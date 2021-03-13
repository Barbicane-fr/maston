package fr.barbicane.maston.topology;

import static java.util.Objects.nonNull;

import fr.barbicane.maston.error.KafkaError;
import fr.barbicane.maston.processor.RecordProcessor;
import java.util.function.Function;

public class DummyValidationProcessor<T> implements RecordProcessor<T> {

  private final Function<T, KafkaError<T>> dummyAvroTestFunction;
  private boolean error = false;
  private KafkaError<T> kafkaError;
  private T dummyAvroTest;

  public DummyValidationProcessor(Function<T, KafkaError<T>> dummyAvroTestFunction) {
    this.dummyAvroTestFunction = dummyAvroTestFunction;
  }

  @Override
  public boolean hasError() {
    return this.error;
  }

  @Override
  public KafkaError<T> getError() {
    return this.kafkaError;
  }

  @Override
  public void process(T dummyAvroTest) {
    final KafkaError<T> kafkaError = dummyAvroTestFunction.apply(dummyAvroTest);
    this.dummyAvroTest = dummyAvroTest;
    if (nonNull(kafkaError)) {
      this.error = true;
      this.kafkaError = kafkaError;
    }
  }

  public T getDummyAvroTest() {
    return this.dummyAvroTest;
  }
}
