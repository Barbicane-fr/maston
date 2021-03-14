package fr.barbicane.maston.serdes;

import static io.vavr.control.Validation.invalid;
import static io.vavr.control.Validation.valid;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import fr.barbicane.maston.error.KafkaError;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import io.vavr.control.Option;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class VavrSafeSerializerTest {

  public static final String MASTON = "maston";
  public static final String MASTON_TOPIC = "maston-topic";
  private SpecificAvroSerializer<SpecificRecord> serializer;
  private VavrSafeSerializer<SpecificRecord> vavrSafeSerializer;
  private SpecificRecord record;

  @BeforeEach
  public void setUp() {
    serializer = mock(SpecificAvroSerializer.class);
    vavrSafeSerializer = new VavrSafeSerializer<>(serializer, SpecificRecord.class);
    record = mock(SpecificRecord.class);
  }


  @Test
  public void should_serialize_valid_validation() {
    final SpecificAvroSerializer<SpecificRecord> serializer = mock(SpecificAvroSerializer.class);
    final VavrSafeSerializer<SpecificRecord> vavrSafeSerializer = new VavrSafeSerializer<>(serializer, SpecificRecord.class);
    final SpecificRecord record = mock(SpecificRecord.class);
    final byte[] bytes = MASTON.getBytes();
    when(serializer.serialize(MASTON_TOPIC, record)).thenReturn(bytes);

    final byte[] tests = vavrSafeSerializer.serialize(MASTON_TOPIC, valid(record));

    assertEquals(bytes, tests);
  }

  @Test
  public void should_send_default_bytes_to_commit_when_no_data_in_error() {
    final byte[] tests = vavrSafeSerializer.serialize(MASTON_TOPIC, invalid(KafkaError.<SpecificRecord>builder().build()));

    assertEquals(0, tests.length);
  }

  @Test
  public void should_serialize_error() {
    final byte[] bytes = MASTON.getBytes();
    when(serializer.serialize(MASTON_TOPIC, record)).thenReturn(bytes);

    final byte[] tests = vavrSafeSerializer.serialize(MASTON_TOPIC, invalid(
        KafkaError.<SpecificRecord>builder().bytes(Option.of(bytes)).build()
    ));

    assertEquals(bytes, tests);
  }

  @Test
  public void should_return_default_bytes_while_handling_serialization_error_on_error() {
    when(serializer.serialize(MASTON_TOPIC, record)).thenThrow(SerializationException.class);

    final byte[] tests = vavrSafeSerializer.serialize(MASTON_TOPIC, invalid(
        KafkaError.<SpecificRecord>builder().sourceRecord(Option.of(record)).build()
    ));

    assertEquals(0, tests.length);
  }

  @Test
  public void should_configure_and_close() {
    vavrSafeSerializer.configure(anyMap(), anyBoolean());
    vavrSafeSerializer.close();
    verify(serializer, times(1)).configure(anyMap(), anyBoolean());
    verify(serializer, times(1)).close();
  }

}
