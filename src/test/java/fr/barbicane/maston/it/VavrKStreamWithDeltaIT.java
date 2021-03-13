package fr.barbicane.maston.it;

import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.ERROR_DESERIALIZATION_CODE;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.ERROR_DESERIALIZATION_MESSAGE;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_APPLICATION_CODE;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_APPLICATION_ID;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_APPLICATION_MESSAGE;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_EXCEPTION_CLASS;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_EXCEPTION_MESSAGE;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_EXCEPTION_STACK_TRACE;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_OFFSET;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_PARTITION;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_TARGET_CLASS;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_TOPIC;
import static fr.barbicane.maston.topology.SimpleVavrStreamConsumerTopologyTest.STRING_DESERIALIZER;
import static fr.barbicane.maston.topology.VavrKStreamWithDeltaTopologyTest.buildRecordForDeltaTests;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.testcontainers.shaded.org.apache.commons.io.FileUtils.deleteDirectory;

import fr.barbicane.maston.KafkaStreamsDecorator;
import fr.barbicane.maston.gen.DummyAvroTest;
import fr.barbicane.maston.serdes.VavrSafeSerdesBuilder;
import fr.barbicane.maston.topology.TopologyBuilder;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class VavrKStreamWithDeltaIT extends AbstractIT {

  private String outputTopic;
  @ClassRule
  public static DockerComposeContainer environment =
      new DockerComposeContainer(
          new File(SimpleVavrStreamConsumerIT.class.getClassLoader().getResource("docker-compose.test.yml").getFile()))
          .withLocalCompose(true)
          .withExposedService("zookeeper_1", 2181)
          .withExposedService("kafka_1", 9092)
          .waitingFor("schema-registry_1", Wait.forHttp("/subjects").forStatusCode(200));

  public void setUp() {
    i++;
    topic = "dummy-topic-" + i;
    errorTopic = "dummy-topic-error-" + i;
    outputTopic = "dummy-topic-output-" + i;
    applicationId = "dummy-streams-it-" + i;
    properties = new Properties();
    producerProperties = new Properties();
    consumerProperties = new Properties();
    properties.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    properties.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    properties.put(APPLICATION_ID_CONFIG, applicationId);
    properties.put(COMMIT_INTERVAL_MS_CONFIG, 100);
    properties.put(STATE_DIR_CONFIG, STATE_STORE_DIR);
    producerProperties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProperties.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    producerProperties.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
    producerProperties.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    producerProperties.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
    consumerProperties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProperties.put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    consumerProperties.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
    consumerProperties.put(GROUP_ID_CONFIG, "dummy-group-1");
    consumerProperties.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    consumerProperties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProperties.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
    consumerProperties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    final Map<String, String> serdesProperties = new HashMap<>();
    serdesProperties.put(SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
    valueSerdes = VavrSafeSerdesBuilder.buildForAvro(DummyAvroTest.class, serdesProperties);
    consumed = Consumed.with(Serdes.String(), valueSerdes)
        .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST);
    produced = Produced.with(Serdes.String(), valueSerdes);
  }

  @AfterAll
  static void cleanStateStoreDir() throws IOException {
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
  public void should_consumed_and_compare_record_and_build_key() throws InterruptedException {
    setUp();

    createTopic(topic);
    createTopic(outputTopic);
    createTopic(errorTopic);
    final Producer<String, DummyAvroTest> producer = new KafkaProducer<String, DummyAvroTest>(producerProperties);

    final List<DummyAvroTest> dummyAvroTests = buildRecordForDeltaTests();

    for (int i = 0; i < dummyAvroTests.size(); i++) {
      producer.send(new ProducerRecord<>(topic, Integer.toString(i), dummyAvroTests.get(i)));
    }

    producer.flush();
    producer.close();

    Thread.sleep(5000);

    final Topology topology = new TopologyBuilder().buildVavrKStreamWithDeltaTopology(
        topic,
        outputTopic,
        Serdes.String(),
        valueSerdes,
        consumed,
        produced,
        () -> r -> r.getMandatoryBusinessStringKey().toString(),
        () -> (oldRecord, newRecord) -> oldRecord.getMandatoryStringValue().toString()
            .contains(newRecord.getMandatoryStringValue().toString()),
        errorTopic
    );

    KafkaStreamsDecorator.start(topology, properties);

    Thread.sleep(2000);

    final Consumer<String, DummyAvroTest> consumer = new KafkaConsumer<String, DummyAvroTest>(consumerProperties);
    consumer.subscribe(Pattern.compile(outputTopic));
    ConsumerRecords<String, DummyAvroTest> poll = consumer.poll(Duration.ofSeconds(5));
    final List<ConsumerRecord<String, DummyAvroTest>> records = new ArrayList<>();
    poll.iterator().forEachRemaining(records::add);

    assertEquals(4, records.size());
    assertEquals(dummyAvroTests.get(0), records.get(0).value());
    assertEquals(dummyAvroTests.get(0).getMandatoryBusinessStringKey().toString(), records.get(0).key());
    assertEquals(dummyAvroTests.get(2), records.get(1).value());
    assertEquals(dummyAvroTests.get(2).getMandatoryBusinessStringKey().toString(), records.get(1).key());
    assertEquals(dummyAvroTests.get(3), records.get(2).value());
    assertEquals(dummyAvroTests.get(3).getMandatoryBusinessStringKey().toString(), records.get(2).key());
    assertEquals(dummyAvroTests.get(5), records.get(3).value());
    assertEquals(dummyAvroTests.get(5).getMandatoryBusinessStringKey().toString(), records.get(3).key());

  }


  @Test
  public void should_handle_deserialization_exception_and_push_to_error_topic() throws InterruptedException {
    setUp();
    createTopic(topic);
    createTopic(outputTopic);
    createTopic(errorTopic);
    final Producer<String, byte[]> producer = new KafkaProducer<String, byte[]>(producerProperties);

    final String key = "test-1";
    final String fakeValue = "record-dummy-test";
    producer.send(new ProducerRecord<>(topic, key, fakeValue.getBytes()));
    // To ensure to write on kafka disk = consumed record headers are readable
    producer.flush();
    producer.close();

    final Topology topology = new TopologyBuilder().buildVavrKStreamWithDeltaTopology(
        topic,
        outputTopic,
        Serdes.String(),
        valueSerdes,
        consumed,
        produced,
        () -> r -> r.getMandatoryBusinessStringKey().toString(),
        () -> (oldRecord, newRecord) -> oldRecord.getMandatoryStringValue().toString()
            .contains(newRecord.getMandatoryStringValue().toString()),
        errorTopic
    );

    KafkaStreamsDecorator.start(topology, properties);

    final Consumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(consumerProperties);
    consumer.subscribe(Pattern.compile(errorTopic));
    ConsumerRecords<String, byte[]> poll = consumer.poll(Duration.ofSeconds(5));

    List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();

    for (ConsumerRecord<String, byte[]> stringConsumerRecord : poll) {
      records.add(stringConsumerRecord);
    }
    assertEquals(1, records.size());
    final ConsumerRecord<String, byte[]> stringConsumerRecord = records.get(0);
    assertEquals(key, stringConsumerRecord.key());
    assertEquals(fakeValue, STRING_DESERIALIZER.deserialize(null, stringConsumerRecord.value()));
    final Headers headers = stringConsumerRecord.headers();
    assertNotNull(headers);
    final Map<String, String> headersDeserialized = new HashMap<>();
    headers.forEach(header -> headersDeserialized.put(header.key(), STRING_DESERIALIZER.deserialize(null, header.value())));

    assertTrue(headersDeserialized.containsKey(HEADER_ERROR_TARGET_CLASS));
    assertEquals(DummyAvroTest.class.getName(), headersDeserialized.get(HEADER_ERROR_TARGET_CLASS));

    assertTrue(headersDeserialized.containsKey(HEADER_ERROR_APPLICATION_ID));
    assertEquals(applicationId, headersDeserialized.get(HEADER_ERROR_APPLICATION_ID));

    assertTrue(headersDeserialized.containsKey(HEADER_ERROR_TOPIC));
    assertEquals(topic, headersDeserialized.get(HEADER_ERROR_TOPIC));

    assertTrue(headersDeserialized.containsKey(HEADER_ERROR_APPLICATION_CODE));
    assertEquals(ERROR_DESERIALIZATION_CODE, headersDeserialized.get(HEADER_ERROR_APPLICATION_CODE));

    assertTrue(headersDeserialized.containsKey(HEADER_ERROR_APPLICATION_MESSAGE));
    assertEquals(ERROR_DESERIALIZATION_MESSAGE, headersDeserialized.get(HEADER_ERROR_APPLICATION_MESSAGE));

    assertTrue(headersDeserialized.containsKey(HEADER_ERROR_EXCEPTION_CLASS));
    assertEquals(ClassCastException.class.getName(), headersDeserialized.get(HEADER_ERROR_EXCEPTION_CLASS));
    assertTrue(headersDeserialized.containsKey(HEADER_ERROR_EXCEPTION_MESSAGE));
    assertTrue(headersDeserialized.containsKey(HEADER_ERROR_EXCEPTION_STACK_TRACE));
    assertTrue(headersDeserialized.get(HEADER_ERROR_EXCEPTION_STACK_TRACE).contains(headersDeserialized.get(HEADER_ERROR_EXCEPTION_CLASS)));

    assertTrue(headersDeserialized.containsKey(HEADER_ERROR_PARTITION));
    assertTrue(headersDeserialized.containsKey(HEADER_ERROR_OFFSET));
    consumer.close();
  }

  protected void createTopic(String topicName) {
    String createTopic =
        String.format(
            "/usr/bin/kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic %s",
            topicName);
    try {
      ContainerState kafka = (ContainerState) environment.getContainerByServiceName("kafka_1").get();
      Container.ExecResult execResult = kafka.execInContainer("/bin/sh", "-c", createTopic);
        if (execResult.getExitCode() != 0) {
            fail();
        }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
