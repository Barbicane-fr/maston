package fr.barbicane.maston.processor;

public interface RecordKeyProcessor<K, V> {


  /**
   * This method is used to provide a new record key for business use case, such as consuming a technical topic with simple incremental
   * integer as key and then building a new key with a business logic such as a business code from the record value.
   * /!\ Changing the key forces a repartitioning !
   *
   * @return new record key
   */
  K buildFromRecord(final V record);
}
