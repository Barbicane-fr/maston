package fr.barbicane.maston.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

public class MetricsBuilder {

  @Getter
  @AllArgsConstructor
  public enum MetricName {
    ERROR_FROM_MASTON_KAFKA_STREAMS("error-from-maston-kafka-streams"),
    ERROR_FROM_RECORD_PROCESSED_BY_APPLICATION("error-from-record-processed-by-application");
    private final String value;
  }


  public static Sensor addCountAndRateErrorSensorToMetricsFromContext(final ProcessorContext ctx, final MetricName metricName) {
    StreamsMetricsImpl streamsMetrics = (StreamsMetricsImpl) ctx.metrics();

    final Sensor invalidRecordSensor = streamsMetrics.taskLevelSensor(
        Thread.currentThread().getName(),
        ctx.taskId().toString(),
        "invalid-record",
        Sensor.RecordingLevel.INFO
    );

    Map<String, String> metricTags = new LinkedHashMap<>();
    metricTags.put("thread-id", Thread.currentThread().getName());
    metricTags.put("task-id", ctx.taskId().toString());
    metricTags.put("application-id", ctx.applicationId());

    final Metrics metrics = new Metrics(new MetricConfig().tags(metricTags));

    invalidRecordSensor.add(
        metrics.metricName(
            metricName.value + "-rate",
            "maston-kafka-streams",
            "The rate of records in error and not computed."
        ),
        new Rate()
    );

    invalidRecordSensor.add(
        metrics.metricName(
            metricName.value + "-count",
            "maston-kafka-streams",
            "The total number of records in error and not computed."
        ),
        new CumulativeCount());
    return invalidRecordSensor;

  }

}
