package fr.barbicane.maston.consumer;

import static fr.barbicane.maston.properties.KafkaPropertiesBuilder.getAvroSerdesConfig;
import static fr.barbicane.maston.properties.KafkaPropertiesBuilder.getKafkaConsumerProperties;

import fr.barbicane.maston.KafkaStreamsDecorator;
import fr.barbicane.maston.error.KafkaError;
import fr.barbicane.maston.processor.RecordDeltaProcessor;
import fr.barbicane.maston.processor.RecordKeyProcessor;
import fr.barbicane.maston.properties.KafkaCommonProperties;
import fr.barbicane.maston.properties.KafkaConsumerProperties;
import fr.barbicane.maston.serdes.VavrSafeSerdesBuilder;
import fr.barbicane.maston.topology.TopologyBuilder;
import io.vavr.control.Validation;
import java.util.function.Supplier;
import javax.validation.constraints.NotNull;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

/**
 * This class is derived from {@link SimpleAvroStreamConsumer} : we use the same kind of error handling around the business record process.
 * This consumer allows to consume a record, then build a new record key if needed we the recordKeyProcessorSupplier, then check if this
 * record was already consume and stored with the new key in the state store and finally apply the recordDeltaProcessorSupplier to update or
 * not the record. The recordKeyProcessorSupplier is used to compute a composite key which forces a repartitioning of the output topic
 * compares to the input topic.
 *
 * @param <T> should extend {@link SpecificRecord} in order to use Avro Schema.
 */
public class AvroDeltaStreamConsumer<T extends SpecificRecord> {

  public void buildAndStartWithValidation(@NotNull final KafkaCommonProperties kafkaCommonProperties,
      @NotNull final KafkaConsumerProperties kafkaConsumerProperties,
      @NotNull final Supplier<RecordKeyProcessor<String, T>> recordKeyProcessorSupplier,
      @NotNull final Supplier<RecordDeltaProcessor<T>> recordDeltaProcessorSupplier,
      @NotNull final Class<T> targetClass,
      @NotNull final String producedTopic) {
    final Serde<Validation<KafkaError<T>, T>> valueSerdes = VavrSafeSerdesBuilder
        .buildForAvro(targetClass, getAvroSerdesConfig(kafkaCommonProperties));
    // First library version forces to use key as string format to avoid different partitioning for
    // same value which can be in different type in different application, such as article ug typed as integer
    // or as string.
    final Consumed<String, Validation<KafkaError<T>, T>> consumed = Consumed.with(Serdes.String(), valueSerdes)
        .withOffsetResetPolicy(
            kafkaConsumerProperties.isAutoOffsetResetEarliest() ?
                Topology.AutoOffsetReset.EARLIEST : Topology.AutoOffsetReset.LATEST);

    final Produced<String, Validation<KafkaError<T>, T>> produced = Produced.with(Serdes.String(), valueSerdes);
    final Topology topology = new TopologyBuilder().buildVavrKStreamWithDeltaTopology(
        kafkaConsumerProperties.getTopic(),
        producedTopic,
        Serdes.String(),
        valueSerdes,
        consumed,
        produced,
        recordKeyProcessorSupplier,
        recordDeltaProcessorSupplier,
        kafkaConsumerProperties.getErrorTopic()
    );
    KafkaStreamsDecorator.start(topology, getKafkaConsumerProperties(kafkaCommonProperties, kafkaConsumerProperties));
  }
}
