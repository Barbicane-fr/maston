package fr.barbicane.maston.serdes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import fr.barbicane.maston.error.KafkaError;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.vavr.control.Validation;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;


public class VavrSafeDeserializerTest {


  public static final String MASTON_TOPIC = "maston-topic";

  @Test
  public void should_deserialize() {
    final SpecificAvroDeserializer<SpecificRecord> deserializer = mock(SpecificAvroDeserializer.class);
    final SpecificRecord specificRecord = mock(SpecificRecord.class);
    final VavrSafeDeserializer<SpecificRecord> vavrSafeDeserializer = new VavrSafeDeserializer<SpecificRecord>(deserializer,
        SpecificRecord.class);

    Mockito.when(deserializer.deserialize(MASTON_TOPIC, new byte[0])).thenReturn(specificRecord);

    final Validation<KafkaError<SpecificRecord>, SpecificRecord> test = vavrSafeDeserializer.deserialize(MASTON_TOPIC, new byte[0]);

    assertNotNull(test);
    assertTrue(test.isValid());
    assertEquals(specificRecord, test.get());
  }

  @Test
  public void should_handle_deserialize_exception() {
    final SpecificAvroDeserializer<SpecificRecord> deserializer = mock(SpecificAvroDeserializer.class);
    final SpecificRecord specificRecord = mock(SpecificRecord.class);
    final VavrSafeDeserializer<SpecificRecord> vavrSafeDeserializer = new VavrSafeDeserializer<SpecificRecord>(deserializer,
        SpecificRecord.class);

    final byte[] bytes = "maston".getBytes();

    Mockito.when(deserializer.deserialize(MASTON_TOPIC, bytes)).thenThrow(SerializationException.class);

    final Validation<KafkaError<SpecificRecord>, SpecificRecord> test = vavrSafeDeserializer.deserialize(MASTON_TOPIC, bytes);

    assertNotNull(test);
    assertTrue(test.isInvalid());
    final KafkaError<SpecificRecord> error = test.getError();
    assertNotNull(error);
    assertEquals(SerializationException.class, error.getThrowable().getClass());
    assertTrue(error.getBytes().isDefined());
    assertEquals(bytes, error.getBytes().get());
  }

  @Test
  public void should_configure_and_close() {
    final SpecificAvroDeserializer<SpecificRecord> deserializer = mock(SpecificAvroDeserializer.class);
    final VavrSafeDeserializer<SpecificRecord> vavrSafeDeserializer = new VavrSafeDeserializer<SpecificRecord>(deserializer,
        SpecificRecord.class);
    vavrSafeDeserializer.configure(anyMap(), anyBoolean());
    vavrSafeDeserializer.close();
    verify(deserializer, times(1)).configure(anyMap(), anyBoolean());
    verify(deserializer, times(1)).close();
  }

}
