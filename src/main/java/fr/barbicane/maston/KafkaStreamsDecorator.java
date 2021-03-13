package fr.barbicane.maston;

import static lombok.AccessLevel.PRIVATE;

import io.vavr.control.Try;
import java.util.Properties;
import javax.validation.constraints.NotNull;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public class KafkaStreamsDecorator {

  public static void start(@NotNull final StreamsBuilder streamsBuilder,
      @NotNull Properties consumerProperties) {
    start(streamsBuilder.build(), consumerProperties);
  }


  public static void start(@NotNull final Topology topology,
      @NotNull Properties consumerProperties) {
    Try.run(() -> {
      LOGGER.info("The following topology was created :\n" + topology.describe());
      final KafkaStreams streams = new KafkaStreams(topology, consumerProperties);
      streams.setUncaughtExceptionHandler((t, e) -> logAndShutdown(e));
      streams.start();
      while (streams.state() != KafkaStreams.State.RUNNING) {
        Thread.sleep(500);
      }
      LOGGER.info(streams.localThreadsMetadata().toString());
      Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }).onFailure(KafkaStreamsDecorator::logAndShutdown);
  }


  private static void logAndShutdown(final Throwable throwable) {
    LOGGER.error("Something went wrong while streaming data from Kafka.", throwable);
    System.exit(1);
  }

}
