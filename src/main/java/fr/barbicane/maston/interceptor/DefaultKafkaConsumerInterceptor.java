package fr.barbicane.maston.interceptor;

import io.vavr.control.Option;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@Slf4j
public class DefaultKafkaConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

  @Override
  public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> consumerRecords) {
    return Option.of(consumerRecords)
        .peek(records ->
            records.forEach(record ->
                LOGGER.info("onConsume topic={} partition={} key={} value={}", record.topic(), record.partition(), record.key(),
                    record.value()))
        )
        .getOrNull();
  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
    // Do nothing
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }
}
