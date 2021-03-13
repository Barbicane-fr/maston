package fr.barbicane.maston.properties;

import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.Singular;

@Data
public class KafkaCommonProperties {

  private String basicAuthCredentialsSource;
  private String bootstrapServers;
  private String securityProtocol;
  private boolean schemaAutoRegister;
  private String schemaRegistryUrl;
  private String schemaRegistryBasicAuthUserInfo;
  private String saslMechanism;
  private String saslJaasConfig;
  private String sslEndpointIdentificationAlgorithm;
  private String applicationId;
  private boolean enabledDefaultProducerInterceptor;
  private boolean enabledDefaultConsumerInterceptor;
  private boolean enabledSensorLogToDebug;
  @Singular
  private Map<String, String> additionalProperties = new HashMap<>();
  private Integer requestTimeoutMs;
  private Integer connectionsMaxIdleMs;
}
