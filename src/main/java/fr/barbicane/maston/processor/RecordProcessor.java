package fr.barbicane.maston.processor;

import fr.barbicane.maston.error.KafkaError;


public interface RecordProcessor<T> {

  /**
   * @return boolean to indicate if an error has occurred while processing the input record consumed, such as we can filtered
   * recordProcessor in success to keep only error record to push them to the error topic.
   */
  default boolean hasError() {
    return getError() != null;
  }

  /**
   * @return KafkaError which should contained error code, error message or even Throwable from the process to compute record in the
   * application, such as this kafkaError will be used to send input record to topic error and add error details to record headers to allow
   * developers to debug error records.
   */
  KafkaError<T> getError();

  /**
   * @param t record message from Kafka Consumer. This method allows application to process the input record consumed. If the process failed
   * with a functional error or a exception, method hasError() must return true and method getError() must return a new KafkaError
   * containing error/exception details.
   */
  void process(T t);
}
