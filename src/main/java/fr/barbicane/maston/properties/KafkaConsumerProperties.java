package fr.barbicane.maston.properties;

import lombok.Data;

@Data
public class KafkaConsumerProperties {

  private String topic;
  private String errorTopic;
  private boolean autoOffsetResetEarliest = true;
  private Boolean recordAsAvro = true;
}
