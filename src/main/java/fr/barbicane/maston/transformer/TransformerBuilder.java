package fr.barbicane.maston.transformer;

import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.buildHeadersFromError;
import static fr.barbicane.maston.metrics.MetricsBuilder.MetricName.ERROR_FROM_MASTON_KAFKA_STREAMS;
import static fr.barbicane.maston.metrics.MetricsBuilder.MetricName.ERROR_FROM_RECORD_PROCESSED_BY_APPLICATION;
import static fr.barbicane.maston.metrics.MetricsBuilder.addCountAndRateErrorSensorToMetricsFromContext;
import static io.vavr.control.Option.of;
import static io.vavr.control.Validation.invalid;
import static io.vavr.control.Validation.valid;
import static java.util.Objects.isNull;

import fr.barbicane.maston.error.KafkaError;
import fr.barbicane.maston.processor.RecordDeltaProcessor;
import fr.barbicane.maston.processor.RecordKeyProcessor;
import fr.barbicane.maston.processor.RecordProcessor;
import io.vavr.control.Validation;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Transformer are between the kafak-streams DSL and the API Processor. It allows us to use API Processor implementation into kafka-streams.
 * We use it to do advanced operation no existing in kafka-streams DSL.
 */
public class TransformerBuilder {

  /**
   * Method to enrich kafkaError with the processorContext of the consumed record, such as adding the original consumed record to push it to
   * the topic error. We also add a custom sensor to count the total number and rate of invalid input record. The Transformer gives us
   * access to the processContext and so to the headers. We enrich this headers with error details.
   */
  public static <K, V> Transformer<K, Validation<KafkaError<V>, V>, KeyValue<K, Validation<KafkaError<V>, V>>>
  validationProcessorTransformer(final RecordProcessor<V> recordProcessor) {
    return new Transformer<K, Validation<KafkaError<V>, V>, KeyValue<K, Validation<KafkaError<V>, V>>>() {

      private Sensor sensor;
      private ProcessorContext ctx;

      @Override
      public void init(ProcessorContext processorContext) {
        this.ctx = processorContext;
        sensor = addCountAndRateErrorSensorToMetricsFromContext(processorContext, ERROR_FROM_RECORD_PROCESSED_BY_APPLICATION);
      }

      @Override
      public KeyValue<K, Validation<KafkaError<V>, V>> transform(K k, Validation<KafkaError<V>, V> vs) {
        if (vs.isInvalid()) {
          return KeyValue.pair(k, vs);
        }
        final V v = vs.get();
        recordProcessor.process(v);
        Validation<KafkaError<V>, V> validation = recordProcessor.hasError() ?
            invalid(recordProcessor.getError())
            : valid(v);

        return KeyValue.pair(
            k,
            validation.mapError(vKafkaError -> {
              vKafkaError.setSourceRecord(of(v));
              sensor.record();
              buildHeadersFromError(vKafkaError, ctx);
              return vKafkaError;
            }));
      }

      @Override
      public void close() {

      }
    };
  }

  /**
   * We add a custom sensor to count the total number and rate of invalid input record. The Transformer gives us access to the
   * processContext and so to the headers. We enrich this headers with error details.
   */
  public static <K, V> Transformer<K, Validation<KafkaError<V>, V>, KeyValue<K, Validation<KafkaError<V>, V>>> headersToValidationTransformer() {
    return new Transformer<K, Validation<KafkaError<V>, V>, KeyValue<K, Validation<KafkaError<V>, V>>>() {

      private ProcessorContext ctx;
      private Sensor sensor;

      @Override
      public void init(ProcessorContext processorContext) {
        this.ctx = processorContext;
        sensor = addCountAndRateErrorSensorToMetricsFromContext(processorContext, ERROR_FROM_MASTON_KAFKA_STREAMS);
      }

      @Override
      public KeyValue<K, Validation<KafkaError<V>, V>> transform(K k, Validation<KafkaError<V>, V> vs) {
        return KeyValue.pair(
            k,
            vs.mapError(v -> {
                  buildHeadersFromError(v, ctx);
                  sensor.record();
                  return v;
                }
            ));
      }

      @Override
      public void close() {
      }
    };
  }

  /**
   * This method is derived from {@link #validationProcessorTransformer}. We add the possibly to use a custom state store to compute
   * a custom delta between to record with the same key. Basically, we use the state store to store the previous state of a record
   * and we compare this previous state with the new one with get from the input topic. We use the computation defined in @param recordDeltaProcessor
   * to update or not the previous record state with the new one.
   * Moreover, we also add a @param recordKeyProcessor to allow a repartitioning of the output record.
   */
  public static <K, V> Transformer<K, Validation<KafkaError<V>, V>, KeyValue<K, Validation<KafkaError<V>, V>>>
  validationProcessorWithDelta(String storeName, RecordDeltaProcessor<V> recordDeltaProcessor,
      RecordKeyProcessor<K, V> recordKeyProcessor) {
    return new Transformer<K, Validation<KafkaError<V>, V>, KeyValue<K, Validation<KafkaError<V>, V>>>() {

      private Sensor sensor;
      private ProcessorContext ctx;
      private KeyValueStore<K, Validation<KafkaError<V>, V>> store;

      @Override
      public void init(ProcessorContext context) {
        this.ctx = context;
        store = (KeyValueStore<K, Validation<KafkaError<V>, V>>) context.getStateStore(storeName);
        sensor = addCountAndRateErrorSensorToMetricsFromContext(context, ERROR_FROM_RECORD_PROCESSED_BY_APPLICATION);
      }

      @Override
      public KeyValue<K, Validation<KafkaError<V>, V>> transform(K k, Validation<KafkaError<V>, V> vs) {
        if (vs.isInvalid()) {
          ctx.forward(k, vs);
        }
        KeyValue<K, Validation<KafkaError<V>, V>> kV = null;
        try {
          final K key = recordKeyProcessor.buildFromRecord(vs.get());
          final Validation<KafkaError<V>, V> existingValue = store.get(key);
          if (isNull(existingValue) || recordDeltaProcessor.isUpdated(existingValue.get(), vs.get())) {
            store.put(key, vs);
            kV = KeyValue.pair(key, vs);
          }
        } catch (Exception e) {
          final KafkaError<V> vKafkaError = KafkaError.<V>builder()
              .sourceRecord(of(vs.get()))
              .throwable(e)
              .message("Something went wrong while reading/writing to state store " + storeName)
              .build();
          sensor.record();
          buildHeadersFromError(vKafkaError, ctx);
          kV = KeyValue.pair(k, invalid(vKafkaError));
        }
        return kV;
      }

      @Override
      public void close() {
      }
    };
  }
}
