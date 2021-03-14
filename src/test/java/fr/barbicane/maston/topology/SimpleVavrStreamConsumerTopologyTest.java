package fr.barbicane.maston.topology;

import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_APPLICATION_CODE;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_APPLICATION_ID;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_APPLICATION_MESSAGE;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_EXCEPTION_STACK_TRACE;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_OFFSET;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_PARTITION;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_TARGET_CLASS;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_TOPIC;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.getStacktrace;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import fr.barbicane.maston.error.KafkaError;
import fr.barbicane.maston.gen.DummyAvroTest;
import fr.barbicane.maston.processor.RecordProcessor;
import fr.barbicane.maston.serdes.VavrSafeSerdesBuilder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import io.vavr.control.Validation;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

@Slf4j
public class SimpleVavrStreamConsumerTopologyTest {

  static Properties DEFAULT_DRIVER_CONFIG;
  static final String TOPIC = "topic-test";
  static final String ERROR_TOPIC = "error-topic-test";
  private TopologyTestDriver driver;
  private DummyValidationProcessor processor;
  private static Map<String, ? extends Serializable> SCHEMA_REGISTRY_CONFIG;
  public static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
  private static final String APPLICATION_ID = "test";

  static {
    DEFAULT_DRIVER_CONFIG = new Properties();
    DEFAULT_DRIVER_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    DEFAULT_DRIVER_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    DEFAULT_DRIVER_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    Map<String, String> map = new HashMap<>();
    map.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy:9999");
    SCHEMA_REGISTRY_CONFIG = map;
  }

  @Test
  public void should_consume_and_process_avro_record_and_handle_and_push_errors_to_error_topics() {
    final Serde<Validation<KafkaError<DummyAvroTest>, DummyAvroTest>> validationSerde =
        VavrSafeSerdesBuilder.buildForAvro(DummyAvroTest.class, SCHEMA_REGISTRY_CONFIG);

    final Serde<DummyAvroTest> dummyAvroTestSerde = Serdes
        .serdeFrom(new SpecificAvroSerializer<DummyAvroTest>(), new SpecificAvroDeserializer<DummyAvroTest>());
    dummyAvroTestSerde.configure(SCHEMA_REGISTRY_CONFIG, false);

    final Function<DummyAvroTest, KafkaError<DummyAvroTest>> mock = mock(Function.class);
    processor = new DummyValidationProcessor(mock);
    Supplier<RecordProcessor<DummyAvroTest>> recordProcessorSupplier = () -> processor;

    final Topology topology = new TopologyBuilder().buildSimpleVavrStreamConsumerTopology(
        TOPIC,
        Consumed.with(Serdes.String(), validationSerde),
        Produced.with(Serdes.String(), validationSerde),
        recordProcessorSupplier,
        ERROR_TOPIC
    ).build();
    driver = new TopologyTestDriver(topology, DEFAULT_DRIVER_CONFIG);

    final TestInputTopic<String, DummyAvroTest> inputTopic = driver.createInputTopic(
        TOPIC,
        new StringSerializer(),
        dummyAvroTestSerde.serializer()
    );

    final TestOutputTopic<String, DummyAvroTest> errortTopic = driver.createOutputTopic(
        ERROR_TOPIC,
        new StringDeserializer(),
        dummyAvroTestSerde.deserializer()
    );

    final DummyAvroTest dummyAvroTest = DummyAvroTest.newBuilder()
        .setMandatoryStringValue("test-111")
        .setNullableStringValue(null)
        .setNullableDummyBigDecimal(null)
        .build();
    final String key = "random-dummy-key";
    when(mock.apply(dummyAvroTest)).thenReturn(null);

    inputTopic.pipeInput(key, dummyAvroTest);

    assertFalse(processor.hasError());
    assertNull(processor.getError());
    assertNotNull(processor.getDummyAvroTest());
    assertEquals(dummyAvroTest, processor.getDummyAvroTest());

    final String appErrorCode = "HTTP_CLIENT_ERROR_TO_GET_SUB_DUMMY";
    final String appErrorMessage = "Failed to get sub dummy from another API.";
    final IOException dummyTestException = new IOException("dummyTestException");
    KafkaError<DummyAvroTest> error = KafkaError.<DummyAvroTest>builder()
        .throwable(dummyTestException)
        .targetClass(DummyAvroTest.class)
        .code(appErrorCode)
        .message(appErrorMessage)
        .build();
    when(mock.apply(dummyAvroTest)).thenReturn(error);

    inputTopic.pipeInput(key, dummyAvroTest);

    assertTrue(processor.hasError());
    assertNotNull(processor.getError());
    KafkaError<DummyAvroTest> errorToTest = processor.getError();
    assertNotNull(errorToTest);
    assertEquals(error.getThrowable(), errorToTest.getThrowable());
    assertEquals(error.getMessage(), errorToTest.getMessage());
    assertEquals(error.getCode(), errorToTest.getCode());
    assertEquals(error.getTargetClass(), errorToTest.getTargetClass());
    assertNotNull(errorToTest.getSourceRecord());
    assertTrue(errorToTest.getSourceRecord().isDefined());
    assertEquals(dummyAvroTest, errorToTest.getSourceRecord().get());

    List<TestRecord<String, DummyAvroTest>> errorRecords = errortTopic.readRecordsToList();
    assertNotNull(errorRecords);
    assertEquals(1, errorRecords.size());
    final TestRecord<String, DummyAvroTest> errorRecord = errorRecords.get(0);
    assertEquals(dummyAvroTest, errorRecord.getValue());
    assertEquals(key, errorRecord.getKey());
    final Headers headers = errorRecord.getHeaders();
    assertNotNull(headers);
    final Header h1 = headers.headers(HEADER_ERROR_TARGET_CLASS).iterator().next();
    assertNotNull(h1);
    assertEquals(DummyAvroTest.class.getName(), toString(h1));
    final Header h2 = headers.headers(HEADER_ERROR_APPLICATION_ID).iterator().next();
    assertNotNull(h2);
    assertEquals(APPLICATION_ID, toString(h2));
    final Header h3 = headers.headers(HEADER_ERROR_TOPIC).iterator().next();
    assertNotNull(h3);
    assertEquals(TOPIC, toString(h3));
    final Header h4 = headers.headers(HEADER_ERROR_APPLICATION_CODE).iterator().next();
    assertNotNull(h4);
    assertEquals(appErrorCode, toString(h4));
    final Header h5 = headers.headers(HEADER_ERROR_APPLICATION_MESSAGE).iterator().next();
    assertNotNull(h5);
    assertEquals(appErrorMessage, toString(h5));
    final Header h6 = headers.headers(HEADER_ERROR_EXCEPTION_STACK_TRACE).iterator().next();
    assertNotNull(h6);
    assertEquals(getStacktrace(dummyTestException), toString(h6));
    final Header h7 = headers.headers(HEADER_ERROR_PARTITION).iterator().next();
    assertNotNull(h7);
    assertEquals("0", toString(h7));
    final Header h8 = headers.headers(HEADER_ERROR_OFFSET).iterator().next();
    assertNotNull(h8);
    assertEquals("1", toString(h8));

    inputTopic.pipeInput(key, null);
    assertNull(processor.getDummyAvroTest());

  }


  public static String toString(final Header h) {
    return STRING_DESERIALIZER.deserialize(null, h.value());
  }
}
