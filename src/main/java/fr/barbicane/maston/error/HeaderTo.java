package fr.barbicane.maston.error;

import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_APPLICATION_CODE;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_APPLICATION_ID;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_APPLICATION_MESSAGE;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_EXCEPTION_CLASS;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_EXCEPTION_MESSAGE;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_EXCEPTION_STACK_TRACE;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_TARGET_CLASS;
import static fr.barbicane.maston.error.KafkaErrorToHeaderBuilder.HEADER_ERROR_TOPIC;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * This transformer can only be used on a kafka-connect connector by adding it to the connector classpath.
 * For more information, please read https://docs.confluent.io/platform/current/connect/transforms/custom.html
 */
@Slf4j
public abstract class HeaderTo<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final String PURPOSE = "Key/value field creation from headers";


  private static Struct structWithAddedFields(Struct struct,
      Headers headers,
      List<Map.Entry<String, String>> headerFieldPairs) {
      if (headers == null) {
          return struct;
      }
    try {
      SchemaBuilder newSchema = SchemaBuilder.struct();
      newSchema.doc(struct.schema().doc());

      struct.schema().fields().forEach(filed -> newSchema.field(filed.name(), filed.schema()));
      for (Map.Entry<String, String> pair : headerFieldPairs) {
        final Header header = headers.lastWithName(pair.getKey());
        if (header != null && pair.getValue() != null && header.schema() != null) {
          newSchema.field(pair.getValue(), header.schema());
        }
      }

      Struct newStruct = new Struct(newSchema.build());
      struct.schema().fields().forEach(filed -> newStruct.put(filed.name(), struct.get(filed)));
      for (Map.Entry<String, String> pair : headerFieldPairs) {
        final Header header = headers.lastWithName(pair.getKey());
        if (header != null && pair.getValue() != null) {
          newStruct.put(pair.getValue(), header.value());
        }
      }
      return newStruct;
    } catch (Exception e) {
      LOGGER.debug("Error while setting headers to field.", e);
    }

    return struct;
  }

  protected final String[] headers = new String[]{
      HEADER_ERROR_TARGET_CLASS,
      HEADER_ERROR_APPLICATION_CODE,
      HEADER_ERROR_APPLICATION_ID,
      HEADER_ERROR_EXCEPTION_MESSAGE,
      HEADER_ERROR_EXCEPTION_STACK_TRACE,
      HEADER_ERROR_TOPIC,
      HEADER_ERROR_APPLICATION_MESSAGE,
      HEADER_ERROR_EXCEPTION_CLASS
  };

  protected final String[] fields = Arrays.stream(headers).map(s -> s.replaceAll("\\.", "_")).toArray(String[]::new);

  protected List<Map.Entry<String, String>> headersFieldsZipped;

  @Override
  public R apply(R record) {
      if (record == null || operatingSchema(record) == null) {
          return record;
      }

    R newRecord = addFields(record);

    Arrays.stream(headers).forEach(header -> newRecord.headers().remove(header));
    return newRecord;
  }

  @Override
  public void close() {
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    headersFieldsZipped = IntStream
        .range(0, headers.length)
        .mapToObj(i -> new AbstractMap.SimpleImmutableEntry<>(headers[i], fields[i]))
        .collect(Collectors.toList());
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R addFields(R record);

  public static class Key<R extends ConnectRecord<R>> extends HeaderTo<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R addFields(R record) {
      Struct oldKey = requireStructOrNull(operatingValue(record), PURPOSE);
      Struct newKey = structWithAddedFields(oldKey, record.headers(), headersFieldsZipped);

      return record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          newKey.schema(),
          newKey,
          record.valueSchema(),
          record.value(),
          record.timestamp(),
          record.headers()
      );
    }
  }

  public static Struct requireStructOrNull(Object value, String purpose) {
    if (value == null) {
      return null;
    }
    if (!(value instanceof Struct)) {
      throw new DataException("Only Struct objects supported for [" + purpose + "], found: " + nullSafeClassName(value));
    } else {
      return (Struct) value;
    }
  }

  private static String nullSafeClassName(Object x) {
    return x == null ? "null" : x.getClass().getCanonicalName();
  }

  public static class Value<R extends ConnectRecord<R>> extends HeaderTo<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R addFields(R record) {
      Struct oldValue = requireStructOrNull(operatingValue(record), PURPOSE);
      Struct newValue = structWithAddedFields(oldValue, record.headers(), headersFieldsZipped);

      return record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          record.keySchema(),
          record.key(),
          newValue.schema(),
          newValue,
          record.timestamp(),
          record.headers()
      );
    }
  }
}