package fr.barbicane.maston.topology;

import static fr.barbicane.maston.transformer.TransformerBuilder.validationProcessorTransformer;
import static fr.barbicane.maston.transformer.TransformerBuilder.validationProcessorWithDelta;
import static java.util.Objects.nonNull;

import fr.barbicane.maston.error.KafkaError;
import fr.barbicane.maston.processor.RecordDeltaProcessor;
import fr.barbicane.maston.processor.RecordKeyProcessor;
import fr.barbicane.maston.processor.RecordProcessor;
import fr.barbicane.maston.transformer.TransformerBuilder;
import io.vavr.control.Validation;
import java.util.function.Supplier;
import javax.validation.constraints.NotNull;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

@SuppressWarnings("unchecked")
public class TopologyBuilder {


  public <K, V> StreamsBuilder buildSimpleVavrStreamConsumerTopology(@NotNull final String consumedTopic,
      @NotNull final Consumed<K, Validation<KafkaError<V>, V>> consumed,
      @NotNull final Produced<K, Validation<KafkaError<V>, V>> produced,
      @NotNull final Supplier<RecordProcessor<V>> recordProcessorSupplier,
      @NotNull final String errorTopic) {
    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<K, Validation<KafkaError<V>, V>>[] branch = builder.stream(consumedTopic, consumed)
        .branch((k, v) -> v.isInvalid(), (k, v) -> v.isValid());

    // Send invalidated message to error topic => KafkaError should contained headers describing the error, root cause, stack trace and so one.
    branch[0]
        .transform(TransformerBuilder::<K, V>headersToValidationTransformer)
        .to(errorTopic, produced);

    branch[1]
        .transform(() -> validationProcessorTransformer(recordProcessorSupplier.get()))
        .filter((k, v) -> v.isInvalid())
        // Send invalidated message to error topic => KafkaError should contained headers describing the error, root cause, stack trace and so one.
        .to(errorTopic, produced);
    return builder;
  }


  public <K, V> Topology buildVavrKStreamWithDeltaTopology(@NotNull final String consumedTopic,
      @NotNull final String producedTopic,
      @NotNull final Serde<K> keySerde,
      @NotNull final Serde<Validation<KafkaError<V>, V>> valueSerde,
      @NotNull final Consumed<K, Validation<KafkaError<V>, V>> consumed,
      @NotNull final Produced<K, Validation<KafkaError<V>, V>> produced,
      @NotNull final Supplier<RecordKeyProcessor<K, V>> recordKeyProcessorSupplier,
      @NotNull final Supplier<RecordDeltaProcessor<V>> recordDeltaProcessorSupplier,
      @NotNull final String errorTopic) {
    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<K, Validation<KafkaError<V>, V>>[] branch = builder.stream(consumedTopic, consumed)
        .branch((k, v) -> v.isInvalid(), (k, v) -> v.isValid());

    StoreBuilder<KeyValueStore<K, Validation<KafkaError<V>, V>>> store = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(producedTopic),
        keySerde, valueSerde
    );

    // Send invalidated message to error topic => KafkaError should contained headers describing the error, root cause, stack trace and so one.
    branch[0]
        .transform(TransformerBuilder::<K, V>headersToValidationTransformer)
        .to(errorTopic, produced);

    final String deltaTransformerName = "maston-kafka-stream-" + producedTopic + "-delta-transformer";
    final KStream<K, Validation<KafkaError<V>, V>>[] errorOrToProducedTopicBranches = branch[1].transform(() ->
            validationProcessorWithDelta(producedTopic, recordDeltaProcessorSupplier.get(), recordKeyProcessorSupplier.get()),
        Named.as(deltaTransformerName))
        .filter((k, vs) -> nonNull(vs))
        .branch((k, vs) -> vs.isInvalid(), (k, vs) -> vs.isValid());

    errorOrToProducedTopicBranches[0].to(errorTopic, produced);
    errorOrToProducedTopicBranches[1].to(producedTopic, produced);

    final Topology topology = builder.build();
    topology.addStateStore(store, deltaTransformerName);

    return topology;
  }

}
