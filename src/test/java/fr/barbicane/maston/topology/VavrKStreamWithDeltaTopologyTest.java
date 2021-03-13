package fr.barbicane.maston.topology;

import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.ERROR_DESERIALIZATION_CODE;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.ERROR_DESERIALIZATION_MESSAGE;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_APPLICATION_CODE;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_APPLICATION_ID;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_APPLICATION_MESSAGE;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_EXCEPTION_STACK_TRACE;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_OFFSET;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_PARTITION;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_TARGET_CLASS;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_TOPIC;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.shaded.org.apache.commons.io.FileUtils.deleteDirectory;

import fr.barbicane.maston.error.KafkaError;
import fr.barbicane.maston.gen.DummyAvroTest;
import fr.barbicane.maston.processor.RecordDeltaProcessor;
import fr.barbicane.maston.processor.RecordKeyProcessor;
import fr.barbicane.maston.serdes.VavrSafeSerdesBuilder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import io.vavr.control.Validation;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

public class VavrKStreamWithDeltaTopologyTest {

  static Properties DEFAULT_DRIVER_CONFIG;
  static final String TOPIC = "topic-test";
  static final String ERROR_TOPIC = "error-topic-test";
  static final String OUTPUT_TOPIC = "output-topic-test";
  private static Map<String, ? extends Serializable> SCHEMA_REGISTRY_CONFIG;
  private static final String APPLICATION_ID = "test";
  protected static final String STATE_STORE_DIR = "/tmp/kafka-streams-unit-test";

  static {
    DEFAULT_DRIVER_CONFIG = new Properties();
    DEFAULT_DRIVER_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    DEFAULT_DRIVER_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    DEFAULT_DRIVER_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    DEFAULT_DRIVER_CONFIG.put("state.dir", STATE_STORE_DIR);
    Map<String, String> map = new HashMap<>();
    map.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy:9999");
    SCHEMA_REGISTRY_CONFIG = map;
  }

  @AfterAll
  static void cleanStateStore() throws IOException {
    final File dirToDelete = new File(STATE_STORE_DIR);
    File[] allContents = dirToDelete.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        deleteDirectory(file);
      }
    }
    dirToDelete.delete();
  }

  @Test
  public void should_handle_consumption_error() {
    DEFAULT_DRIVER_CONFIG.replace(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID + "-error");
    final Serde<Validation<KafkaError<DummyAvroTest>, DummyAvroTest>> validationSerde =
        VavrSafeSerdesBuilder.buildForAvro(DummyAvroTest.class, SCHEMA_REGISTRY_CONFIG);

    final Serde<DummyAvroTest> dummyAvroTestSerde = Serdes
        .serdeFrom(new SpecificAvroSerializer<DummyAvroTest>(), new SpecificAvroDeserializer<DummyAvroTest>());
    dummyAvroTestSerde.configure(SCHEMA_REGISTRY_CONFIG, false);

    final Supplier<RecordDeltaProcessor<DummyAvroTest>> recordDeltaProcessorSupplier =
        () -> (oldRecord, newRecord) -> oldRecord.getMandatoryStringValue().toString()
            .contains(newRecord.getMandatoryStringValue().toString());

    final Supplier<RecordKeyProcessor<String, DummyAvroTest>> recordKeyProcessorSupplier =
        () -> record -> record.getMandatoryBusinessStringKey().toString();

    final Topology topology = new TopologyBuilder().buildVavrKStreamWithDeltaTopology(
        TOPIC,
        OUTPUT_TOPIC,
        Serdes.String(),
        validationSerde,
        Consumed.with(Serdes.String(), validationSerde),
        Produced.with(Serdes.String(), validationSerde),
        recordKeyProcessorSupplier,
        recordDeltaProcessorSupplier,
        ERROR_TOPIC
    );
    System.out.println(topology.describe());
    final TopologyTestDriver driver = new TopologyTestDriver(topology, DEFAULT_DRIVER_CONFIG);

    final TestInputTopic<String, String> inputTopic = driver.createInputTopic(
        TOPIC,
        new StringSerializer(),
        new StringSerializer()
    );

    final TestOutputTopic<String, String> errorTopic = driver.createOutputTopic(
        ERROR_TOPIC,
        new StringDeserializer(),
        new StringDeserializer()
    );

    final TestOutputTopic<String, DummyAvroTest> outputTopic = driver.createOutputTopic(
        OUTPUT_TOPIC,
        new StringDeserializer(),
        dummyAvroTestSerde.deserializer()
    );

    inputTopic.pipeInput("test-key", "test-value");

    assertFalse(errorTopic.isEmpty());
    assertTrue(outputTopic.isEmpty());
    final List<TestRecord<String, String>> testRecords = errorTopic.readRecordsToList();
    assertEquals(1, testRecords.size());

    final TestRecord<String, String> testRecord = testRecords.get(0);

    assertEquals("test-value", testRecord.getValue());
    assertEquals("test-key", testRecord.getKey());
    final Headers headers = testRecord.getHeaders();
    assertNotNull(headers);
    final Header h1 = headers.headers(HEADER_ERROR_TARGET_CLASS).iterator().next();
    assertNotNull(h1);
    assertEquals(DummyAvroTest.class.getName(), SimpleVavrStreamConsumerTopologyTest.toString(h1));
    final Header h2 = headers.headers(HEADER_ERROR_APPLICATION_ID).iterator().next();
    assertNotNull(h2);
    assertEquals(APPLICATION_ID + "-error", SimpleVavrStreamConsumerTopologyTest.toString(h2));
    final Header h3 = headers.headers(HEADER_ERROR_TOPIC).iterator().next();
    assertNotNull(h3);
    assertEquals(TOPIC, SimpleVavrStreamConsumerTopologyTest.toString(h3));
    final Header h4 = headers.headers(HEADER_ERROR_APPLICATION_CODE).iterator().next();
    assertNotNull(h4);
    assertEquals(ERROR_DESERIALIZATION_CODE, SimpleVavrStreamConsumerTopologyTest.toString(h4));
    final Header h5 = headers.headers(HEADER_ERROR_APPLICATION_MESSAGE).iterator().next();
    assertNotNull(h5);
    assertEquals(ERROR_DESERIALIZATION_MESSAGE, SimpleVavrStreamConsumerTopologyTest.toString(h5));
    final Header h6 = headers.headers(HEADER_ERROR_EXCEPTION_STACK_TRACE).iterator().next();
    assertNotNull(h6);
    final Header h7 = headers.headers(HEADER_ERROR_PARTITION).iterator().next();
    assertNotNull(h7);
    assertEquals("0", SimpleVavrStreamConsumerTopologyTest.toString(h7));
    final Header h8 = headers.headers(HEADER_ERROR_OFFSET).iterator().next();
    assertNotNull(h8);
    assertEquals("0", SimpleVavrStreamConsumerTopologyTest.toString(h8));
  }


  @Test
  public void should_handle_client_exception() {
    DEFAULT_DRIVER_CONFIG.replace(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID + "-exception");
    final Serde<Validation<KafkaError<DummyAvroTest>, DummyAvroTest>> validationSerde =
        VavrSafeSerdesBuilder.buildForAvro(DummyAvroTest.class, SCHEMA_REGISTRY_CONFIG);

    final Serde<DummyAvroTest> dummyAvroTestSerde = Serdes
        .serdeFrom(new SpecificAvroSerializer<DummyAvroTest>(), new SpecificAvroDeserializer<DummyAvroTest>());
    dummyAvroTestSerde.configure(SCHEMA_REGISTRY_CONFIG, false);

    final Supplier<RecordDeltaProcessor<DummyAvroTest>> recordDeltaProcessorSupplier =
        () -> (oldRecord, newRecord) -> oldRecord.getMandatoryStringValue().toString()
            .contains(newRecord.getMandatoryStringValue().toString());

    final Supplier<RecordKeyProcessor<String, DummyAvroTest>> recordKeyProcessorSupplier =
        () -> record -> {
          Integer i = null;
          return i.toString();
        };

    final Topology topology = new TopologyBuilder().buildVavrKStreamWithDeltaTopology(
        TOPIC,
        OUTPUT_TOPIC,
        Serdes.String(),
        validationSerde,
        Consumed.with(Serdes.String(), validationSerde),
        Produced.with(Serdes.String(), validationSerde),
        recordKeyProcessorSupplier,
        recordDeltaProcessorSupplier,
        ERROR_TOPIC
    );

    final TopologyTestDriver driver = new TopologyTestDriver(topology, DEFAULT_DRIVER_CONFIG);

    final TestInputTopic<String, DummyAvroTest> inputTopic = driver.createInputTopic(
        TOPIC,
        new StringSerializer(),
        dummyAvroTestSerde.serializer()
    );

    final TestOutputTopic<String, DummyAvroTest> errorTopic = driver.createOutputTopic(
        ERROR_TOPIC,
        new StringDeserializer(),
        dummyAvroTestSerde.deserializer()
    );

    final TestOutputTopic<String, DummyAvroTest> outputTopic = driver.createOutputTopic(
        OUTPUT_TOPIC,
        new StringDeserializer(),
        dummyAvroTestSerde.deserializer()
    );

    final DummyAvroTest dummyAvroTest = DummyAvroTest.newBuilder()
        .setMandatoryStringValue("test-value")
        .setMandatoryBusinessStringKey("test-new-key").build();

    inputTopic.pipeInput("test-key", dummyAvroTest);

    assertFalse(errorTopic.isEmpty());
    assertTrue(outputTopic.isEmpty());
    final List<TestRecord<String, DummyAvroTest>> testRecords = errorTopic.readRecordsToList();
    assertEquals(1, testRecords.size());

    final TestRecord<String, DummyAvroTest> testRecord = testRecords.get(0);

    assertEquals(dummyAvroTest, testRecord.getValue());
    assertEquals("test-key", testRecord.getKey());
    final Headers headers = testRecord.getHeaders();
    assertNotNull(headers);

    final Header h2 = headers.headers(HEADER_ERROR_APPLICATION_ID).iterator().next();
    assertNotNull(h2);
    assertEquals(APPLICATION_ID + "-exception", SimpleVavrStreamConsumerTopologyTest.toString(h2));
    final Header h3 = headers.headers(HEADER_ERROR_TOPIC).iterator().next();
    assertNotNull(h3);
    assertEquals(TOPIC, SimpleVavrStreamConsumerTopologyTest.toString(h3));
    final Header h6 = headers.headers(HEADER_ERROR_EXCEPTION_STACK_TRACE).iterator().next();
    assertNotNull(h6);
    final Header h7 = headers.headers(HEADER_ERROR_PARTITION).iterator().next();
    assertNotNull(h7);
    assertEquals("0", SimpleVavrStreamConsumerTopologyTest.toString(h7));
    final Header h8 = headers.headers(HEADER_ERROR_OFFSET).iterator().next();
    assertNotNull(h8);
    assertEquals("0", SimpleVavrStreamConsumerTopologyTest.toString(h8));
  }


  @Test
  public void should_consume_record_and_compare_existing_and_new_value_and_change_key() {
    DEFAULT_DRIVER_CONFIG.replace(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID + "-normal");
    final Serde<Validation<KafkaError<DummyAvroTest>, DummyAvroTest>> validationSerde =
        VavrSafeSerdesBuilder.buildForAvro(DummyAvroTest.class, SCHEMA_REGISTRY_CONFIG);

    final Serde<DummyAvroTest> dummyAvroTestSerde = Serdes
        .serdeFrom(new SpecificAvroSerializer<DummyAvroTest>(), new SpecificAvroDeserializer<DummyAvroTest>());
    dummyAvroTestSerde.configure(SCHEMA_REGISTRY_CONFIG, false);

    final Supplier<RecordDeltaProcessor<DummyAvroTest>> recordDeltaProcessorSupplier =
        () -> (oldRecord, newRecord) -> oldRecord.getMandatoryStringValue().toString()
            .contains(newRecord.getMandatoryStringValue().toString());

    final Supplier<RecordKeyProcessor<String, DummyAvroTest>> recordKeyProcessorSupplier =
        () -> record -> record.getMandatoryBusinessStringKey().toString();

    final Topology topology = new TopologyBuilder().buildVavrKStreamWithDeltaTopology(
        TOPIC,
        OUTPUT_TOPIC,
        Serdes.String(),
        validationSerde,
        Consumed.with(Serdes.String(), validationSerde),
        Produced.with(Serdes.String(), validationSerde),
        recordKeyProcessorSupplier,
        recordDeltaProcessorSupplier,
        ERROR_TOPIC
    );

    final TopologyTestDriver driver = new TopologyTestDriver(topology, DEFAULT_DRIVER_CONFIG);

    final TestInputTopic<String, DummyAvroTest> inputTopic = driver.createInputTopic(
        TOPIC,
        new StringSerializer(),
        dummyAvroTestSerde.serializer()
    );

    final TestOutputTopic<String, DummyAvroTest> errorTopic = driver.createOutputTopic(
        ERROR_TOPIC,
        new StringDeserializer(),
        dummyAvroTestSerde.deserializer()
    );

    final TestOutputTopic<String, DummyAvroTest> outputTopic = driver.createOutputTopic(
        OUTPUT_TOPIC,
        new StringDeserializer(),
        dummyAvroTestSerde.deserializer()
    );

    final List<DummyAvroTest> dummyAvroTests = buildRecordForDeltaTests();
    for (int i = 0; i < dummyAvroTests.size(); i++) {
      inputTopic.pipeInput(Integer.toString(i), dummyAvroTests.get(i));
    }

    assertTrue(errorTopic.isEmpty());
    assertFalse(outputTopic.isEmpty());
    final List<KeyValue<String, DummyAvroTest>> keyValues = outputTopic.readKeyValuesToList();
    assertEquals(4, keyValues.size());

    assertEquals(dummyAvroTests.get(0), keyValues.get(0).value);
    assertEquals(dummyAvroTests.get(0).getMandatoryBusinessStringKey().toString(), keyValues.get(0).key);
    assertEquals(dummyAvroTests.get(2), keyValues.get(1).value);
    assertEquals(dummyAvroTests.get(2).getMandatoryBusinessStringKey().toString(), keyValues.get(1).key);
    assertEquals(dummyAvroTests.get(3), keyValues.get(2).value);
    assertEquals(dummyAvroTests.get(3).getMandatoryBusinessStringKey().toString(), keyValues.get(2).key);
    assertEquals(dummyAvroTests.get(5), keyValues.get(3).value);
    assertEquals(dummyAvroTests.get(5).getMandatoryBusinessStringKey().toString(), keyValues.get(3).key);

  }


  public static List<DummyAvroTest> buildRecordForDeltaTests() {
    final List<DummyAvroTest> records = new ArrayList<>();

    records.add(
        DummyAvroTest.newBuilder()
            .setMandatoryBusinessStringKey("key-1")
            .setMandatoryStringValue("should_pass")
            .build()
    );
    records.add(
        DummyAvroTest.newBuilder()
            .setMandatoryBusinessStringKey("key-1")
            .setMandatoryStringValue("should_not_pass")
            .build()
    );
    records.add(
        DummyAvroTest.newBuilder()
            .setMandatoryBusinessStringKey("key-1")
            .setMandatoryStringValue("should_pass")
            .build()
    );

    records.add(
        DummyAvroTest.newBuilder()
            .setMandatoryBusinessStringKey("key-2")
            .setMandatoryStringValue("should_pass")
            .build()
    );
    records.add(
        DummyAvroTest.newBuilder()
            .setMandatoryBusinessStringKey("key-2")
            .setMandatoryStringValue("should_not_pass")
            .build()
    );
    records.add(
        DummyAvroTest.newBuilder()
            .setMandatoryBusinessStringKey("key-2")
            .setMandatoryStringValue("should_pass")
            .build()
    );

    return records;
  }
}
