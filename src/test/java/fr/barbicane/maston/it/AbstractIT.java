package fr.barbicane.maston.it;

import fr.barbicane.maston.error.KafkaError;
import fr.barbicane.maston.gen.DummyAvroTest;
import io.vavr.control.Validation;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractIT.class);
  protected static final String BOOTSTRAP_SERVER = "localhost:9092";
  protected static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
  protected String topic;
  protected String errorTopic;
  protected Properties properties;
  protected Properties producerProperties;
  protected Properties consumerProperties;
  protected String applicationId;
  protected static int i = 0;
  protected static final String STATE_STORE_DIR;

  static {
    try {
      STATE_STORE_DIR = Files.createTempDirectory("maston-kafka-streams-intregration-test").toAbsolutePath().toString();
    } catch (IOException e) {
      LOGGER.error("Error while creating temp directory for it tests", e);
      throw new RuntimeException(e);
    }
  }

  protected Serde<Validation<KafkaError<DummyAvroTest>, DummyAvroTest>> valueSerdes;
  protected Consumed<String, Validation<KafkaError<DummyAvroTest>, DummyAvroTest>> consumed;
  protected Produced<String, Validation<KafkaError<DummyAvroTest>, DummyAvroTest>> produced;

}
