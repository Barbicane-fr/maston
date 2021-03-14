package fr.barbicane.maston.error;

import static java.util.Objects.nonNull;

import io.vavr.control.Option;
import java.io.PrintWriter;
import java.io.StringWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
public class KafkaErrorToHeaderBuilder {

  public static final String HEADER_ERROR_APPLICATION_ID = "error.source.application.id";
  public static final String HEADER_ERROR_TIMESTAMP = "error.source.timestamp";
  public static final String HEADER_ERROR_TOPIC = "error.source.topic";
  public static final String HEADER_ERROR_PARTITION = "error.source.partition";
  public static final String HEADER_ERROR_OFFSET = "error.source.partition.offset";
  public static final String HEADER_ERROR_APPLICATION_CODE = "error.source.application.code";
  public static final String HEADER_ERROR_APPLICATION_MESSAGE = "error.source.application.message";
  public static final String HEADER_ERROR_TARGET_CLASS = "error.source.target.class";
  public static final String HEADER_ERROR_EXCEPTION_STACK_TRACE = "error.source.exception.stack.trace";
  public static final String HEADER_ERROR_EXCEPTION_MESSAGE = "error.source.exception.message";
  public static final String HEADER_ERROR_EXCEPTION_CLASS = "error.source.exception.class";

  public static final String ERROR_DESERIALIZATION_CODE = "FAILED_TO_DESERIALIZE_RECORD";
  public static final String ERROR_DESERIALIZATION_MESSAGE = "An exception occurred while deserializing record." +
      "\nCheck safe vavr deserializer and input record format pushed in error topic.";

  public static final String ERROR_SERIALIZATION_CODE = "FAILED_TO_SERIALIZE_RECORD";
  public static final String ERROR_SERIALIZATION_MESSAGE = "An exception occurred while serializing record." +
      "\nCheck safe vavr serializer and input record format pushed in error topic.";


  private static final StringSerializer STRING_SERIALIZER = new StringSerializer();

  public static <T> void buildHeadersFromError(final KafkaError<T> kafkaError, final ProcessorContext ctx) {
    final Headers headers = ctx.headers();
    flushHeaders(headers);
    writeHeaders(kafkaError, ctx, headers);
  }

  private static <T> void writeHeaders(KafkaError<T> kafkaError, ProcessorContext ctx, Headers headers) {
    headers.add(HEADER_ERROR_APPLICATION_ID, toByteArray(ctx.applicationId()));
    headers.add(HEADER_ERROR_TIMESTAMP, toByteArray(Long.toString(ctx.timestamp())));
    headers.add(HEADER_ERROR_TOPIC, toByteArray(ctx.topic()));
    headers.add(HEADER_ERROR_PARTITION, toByteArray(Integer.toString(ctx.partition())));
    headers.add(HEADER_ERROR_OFFSET, toByteArray(Long.toString(ctx.offset())));
    if (nonNull(kafkaError.getTargetClass())) {
      headers.add(HEADER_ERROR_TARGET_CLASS, toByteArray(kafkaError.getTargetClass().getName()));
    }

    LOGGER.debug("Something happened while consuming and processing record. KafkaError = {}", kafkaError);
    Option.of(kafkaError.getCode()).peek(s -> headers.add(HEADER_ERROR_APPLICATION_CODE, toByteArray(s)));
    Option.of(kafkaError.getMessage()).peek(s -> headers.add(HEADER_ERROR_APPLICATION_MESSAGE, toByteArray(s)));

    Option.of(kafkaError.getThrowable()).peek(t -> {
      final String stacktrace = getStacktrace(t);
      LOGGER.debug("Stacktrace = {}", stacktrace);
      headers.add(HEADER_ERROR_EXCEPTION_STACK_TRACE, toByteArray(stacktrace));
      headers.add(HEADER_ERROR_EXCEPTION_MESSAGE, toByteArray(t.getMessage()));
      headers.add(HEADER_ERROR_EXCEPTION_CLASS, toByteArray(t.getClass().getName()));
    });
  }

  private static void flushHeaders(Headers headers) {
    headers.remove(HEADER_ERROR_APPLICATION_ID);
    headers.remove(HEADER_ERROR_TIMESTAMP);
    headers.remove(HEADER_ERROR_TOPIC);
    headers.remove(HEADER_ERROR_PARTITION);
    headers.remove(HEADER_ERROR_OFFSET);
    headers.remove(HEADER_ERROR_TARGET_CLASS);
    headers.remove(HEADER_ERROR_APPLICATION_CODE);
    headers.remove(HEADER_ERROR_APPLICATION_MESSAGE);
    headers.remove(HEADER_ERROR_EXCEPTION_STACK_TRACE);
    headers.remove(HEADER_ERROR_EXCEPTION_MESSAGE);
    headers.remove(HEADER_ERROR_EXCEPTION_CLASS);
  }

  private static byte[] toByteArray(final String data) {
    return STRING_SERIALIZER.serialize(null, data);
  }

  public static String getStacktrace(final Throwable throwable) {
    final StringWriter sw = new StringWriter();
    final PrintWriter pw = new PrintWriter(sw, true);
    throwable.printStackTrace(pw);
    return sw.getBuffer().toString();
  }

}
