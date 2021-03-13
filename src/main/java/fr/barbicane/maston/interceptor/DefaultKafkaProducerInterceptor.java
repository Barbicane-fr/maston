package fr.barbicane.maston.interceptor;

import io.vavr.control.Option;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

@Slf4j
public class DefaultKafkaProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

  @Override
  public ProducerRecord<K, V> onSend(ProducerRecord<K, V> producerRecord) {
    return Option.of(producerRecord)
        .peek(record -> LOGGER.info("onSend topic={} partition={} key={} value={}", record.topic(),
            record.partition(), record.key(), record.value()))
        .getOrNull();
  }

  @Override
  public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
    // Debug level to avoid to over log
    LOGGER.debug("onAck topic={}, partition={}, offset={}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }
}
