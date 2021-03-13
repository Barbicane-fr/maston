package fr.barbicane.maston.processor;

public interface RecordDeltaProcessor<R> {

  /**
   * oldRecord and newRecord are two record value with the same key and they are compared to compute if the newRecord should be put in the
   * state store.
   *
   * @param oldRecord value already contained in the consumed topic and so in the state store.
   * @param newRecord new value consumed.
   * @return true if the newRecord must be updated in the state store, false if not.
   */
  boolean isUpdated(final R oldRecord, final R newRecord);
}
