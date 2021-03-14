package fr.barbicane.maston.consumer;

import static fr.barbicane.maston.properties.KafkaPropertiesBuilder.getAvroSerdesConfig;
import static fr.barbicane.maston.properties.KafkaPropertiesBuilder.getKafkaConsumerProperties;

import fr.barbicane.maston.KafkaStreamsDecorator;
import fr.barbicane.maston.error.KafkaError;
import fr.barbicane.maston.processor.RecordProcessor;
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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;


/**
 * SimpleStreamConsumer allows application to start a simple kafka stream consumer to consume a topic with errors handling, serialization
 * and deserialization from/to Apache Avro format, metrics exposition and a bundle kafka configuration with recommended properties from
 * Confluent. This implementation uses Vavr to wrap error and allows application to push technical error from serdes or functional error
 * from domain and business to an error topic.
 */
public class SimpleAvroStreamConsumer<T extends SpecificRecord> {

  public void buildAndStartWithValidation(@NotNull final KafkaCommonProperties kafkaCommonProperties,
      @NotNull final KafkaConsumerProperties kafkaConsumerProperties,
      @NotNull final Supplier<RecordProcessor<T>> recordProcessorSupplier,
      @NotNull final Class<T> targetClass) {
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
    final StreamsBuilder streamsBuilder = new TopologyBuilder()
        .buildSimpleVavrStreamConsumerTopology(kafkaConsumerProperties.getTopic(), consumed,
            produced, recordProcessorSupplier, kafkaConsumerProperties.getErrorTopic());
    KafkaStreamsDecorator.start(streamsBuilder, getKafkaConsumerProperties(kafkaCommonProperties, kafkaConsumerProperties));

  }

}
