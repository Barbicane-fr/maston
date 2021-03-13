package fr.barbicane.maston.example;

import fr.barbicane.maston.consumer.SimpleAvroStreamConsumer;
import fr.barbicane.maston.error.KafkaError;
import fr.barbicane.maston.gen.DummyAvroTest;
import fr.barbicane.maston.processor.RecordProcessor;
import fr.barbicane.maston.properties.KafkaCommonProperties;
import fr.barbicane.maston.properties.KafkaConsumerProperties;


public class DummyConsumerTest {


  public static final String APPLICATION_ID = "local-test-it-maston-kafka-streams-1";
  public static final String BOOTSTRAP_SERVERS = "xxxx.confluent.cloud:9092";
  public static final String SCHEMA_REGISTRY_URL = "https://xxxx.confluent.cloud";
  public static final String SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO = "registry-user:registry-password";
  public static final String SASL_JAAS_CONFIG = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"password\";";
  public static final String TOPIC = "local_it_dummy_consumer_test";
  public static final String ERROR_TOPIC = "local_it_dummy_consumer_test_error";

  public static void main(String... args) {
    final SimpleAvroStreamConsumer<DummyAvroTest> specificRecordSimpleAvroStreamConsumer = new SimpleAvroStreamConsumer<>();

    final KafkaCommonProperties kafkaCommonProperties = new KafkaCommonProperties();
    final KafkaConsumerProperties kafkaConsumerProperties = new KafkaConsumerProperties();

    kafkaCommonProperties.setApplicationId(APPLICATION_ID);
    kafkaCommonProperties.setBootstrapServers(BOOTSTRAP_SERVERS);
    kafkaCommonProperties.setSecurityProtocol("SASL_SSL");
    kafkaCommonProperties.setBasicAuthCredentialsSource("USER_INFO");
    kafkaCommonProperties.setSchemaAutoRegister(false);
    kafkaCommonProperties.setSaslMechanism("PLAIN");
    kafkaCommonProperties.setSchemaRegistryUrl(SCHEMA_REGISTRY_URL);
    kafkaCommonProperties.setSslEndpointIdentificationAlgorithm("https");
    kafkaCommonProperties
        .setSchemaRegistryBasicAuthUserInfo(SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO);
    kafkaCommonProperties.setSaslJaasConfig(
        SASL_JAAS_CONFIG);
    kafkaCommonProperties.setEnabledDefaultConsumerInterceptor(false);
    kafkaCommonProperties.setEnabledDefaultProducerInterceptor(false);
    kafkaCommonProperties.setEnabledSensorLogToDebug(true);

    kafkaConsumerProperties.setAutoOffsetResetEarliest(true);
    kafkaConsumerProperties.setTopic(TOPIC);
    kafkaConsumerProperties.setErrorTopic(ERROR_TOPIC);

    specificRecordSimpleAvroStreamConsumer.buildAndStartWithValidation(
        kafkaCommonProperties,
        kafkaConsumerProperties,
        () -> new RecordProcessor<DummyAvroTest>() {

          private KafkaError<DummyAvroTest> error;

          @Override
          public boolean hasError() {
            return true;
          }

          @Override
          public KafkaError<DummyAvroTest> getError() {
            return error;
          }

          @Override
          public void process(DummyAvroTest rmsLocationFamilyRelation) {
            this.error = KafkaError.<DummyAvroTest>builder()
                .targetClass(DummyAvroTest.class)
                .code("dummy-code")
                .message("dummy-error-message")
                .build();
          }
        },
        DummyAvroTest.class
    );
  }
}
