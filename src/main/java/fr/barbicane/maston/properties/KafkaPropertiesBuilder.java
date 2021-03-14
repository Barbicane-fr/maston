package fr.barbicane.maston.properties;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG;
import static java.util.Objects.nonNull;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.CONSUMER_PREFIX;
import static org.apache.kafka.streams.StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.OPTIMIZE;
import static org.apache.kafka.streams.StreamsConfig.PRODUCER_PREFIX;
import static org.apache.kafka.streams.StreamsConfig.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.TOPOLOGY_OPTIMIZATION;

import fr.barbicane.maston.interceptor.DefaultKafkaConsumerInterceptor;
import fr.barbicane.maston.interceptor.DefaultKafkaProducerInterceptor;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.StreamsConfig;

public class KafkaPropertiesBuilder {

  public static Properties getKafkaConsumerProperties(final KafkaCommonProperties kafkaCommonProperties,
      final KafkaConsumerProperties kafkaConsumerProperties) {
    final Properties properties = getKafkaCommonProperties(kafkaCommonProperties);
    // Can be override for specific consumer
    properties.putAll(kafkaCommonProperties.getAdditionalProperties());
    return properties;
  }

  public static Map<String, ? extends Serializable> getAvroSerdesConfig(final KafkaCommonProperties kafkaCommonProperties) {
    final Map<String, String> map = new HashMap<>();
    map.put(
        SCHEMA_REGISTRY_URL_CONFIG, kafkaCommonProperties.getSchemaRegistryUrl()
    );
    map.put(
        USER_INFO_CONFIG, kafkaCommonProperties.getSchemaRegistryBasicAuthUserInfo()
    );
    map.put(
        BASIC_AUTH_CREDENTIALS_SOURCE, kafkaCommonProperties.getBasicAuthCredentialsSource()
    );
    return map;
  }

  public static Properties getKafkaCommonProperties(final KafkaCommonProperties kafkaCommonProperties) {
    final Properties props = new Properties();
    props.put(BOOTSTRAP_SERVERS_CONFIG, kafkaCommonProperties.getBootstrapServers());
    props.put(SECURITY_PROTOCOL_CONFIG, kafkaCommonProperties.getSecurityProtocol());
    props.put(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, kafkaCommonProperties.getSslEndpointIdentificationAlgorithm());
    props.put(SASL_JAAS_CONFIG, kafkaCommonProperties.getSaslJaasConfig());
    props.put(SASL_MECHANISM, kafkaCommonProperties.getSaslMechanism());
    props.put(BASIC_AUTH_CREDENTIALS_SOURCE, kafkaCommonProperties.getBasicAuthCredentialsSource());
    props.put(USER_INFO_CONFIG, kafkaCommonProperties.getSchemaRegistryBasicAuthUserInfo());
    props.put(SCHEMA_REGISTRY_URL_CONFIG, kafkaCommonProperties.getSchemaRegistryUrl());
    props.put(AUTO_REGISTER_SCHEMAS, kafkaCommonProperties.isSchemaAutoRegister());
    props.put(APPLICATION_ID_CONFIG, kafkaCommonProperties.getApplicationId());
    // One stream thread by application. If the stream thread is dead, we kill the application to restart it.
    props.put(NUM_STREAM_THREADS_CONFIG, 1);
    props.put(TOPOLOGY_OPTIMIZATION, OPTIMIZE);
    // To consume only committed record
    props.put("isolation.level", "read_committed");
    // Enable Confluent Metrics Reporter to Control Center
    props.put(METRIC_REPORTER_CLASSES_CONFIG, JmxReporter.class.getName());

    if (kafkaCommonProperties.isEnabledDefaultConsumerInterceptor()) {
      props.put(CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, DefaultKafkaConsumerInterceptor.class.getName());
    }
    if (kafkaCommonProperties.isEnabledDefaultProducerInterceptor()) {
      props.put(PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, DefaultKafkaProducerInterceptor.class.getName());
    }

    if (kafkaCommonProperties.isEnabledSensorLogToDebug()) {
      props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.toString());
    }

    // See https://docs.confluent.io/current/cloud/cp-component/streams-cloud-config.html
    props.put(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG), 2147483647);
    props.put("producer.confluent.batch.expiry.ms", 9223372036854775807L);
    props.put("commit.interval.ms", 5000);

    props.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), 9223372036854775807L);
    if (nonNull(kafkaCommonProperties.getRequestTimeoutMs())) {
      props.put(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), kafkaCommonProperties.getRequestTimeoutMs());
    } else {
      props.put(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), 300000);
    }
    if (nonNull(kafkaCommonProperties.getConnectionsMaxIdleMs())) {
      props.put(StreamsConfig.producerPrefix(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
          kafkaCommonProperties.getConnectionsMaxIdleMs());
    } else {
      props.put(StreamsConfig.producerPrefix(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), 540000);
    }

    return props;
  }

}
