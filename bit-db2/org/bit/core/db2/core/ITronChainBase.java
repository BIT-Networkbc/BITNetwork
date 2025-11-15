package org.bit.core.db2.core;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Map.Entry;
import org.bit.common.utils.Quitable;
import org.bit.core.exception.BadItemException;
import org.bit.core.exception.ItemNotFoundException;

public interface IBitChainBase<T> extends Iterable<Entry<byte[], T>>, Quitable {

  /**
   * reset the database.
   */
  void reset();

  /**
   * close the database.
   */
  void close();

  void put(byte[] key, T item);

  void delete(byte[] key);

  T get(byte[] key) throws InvalidProtocolBufferException, ItemNotFoundException, BadItemException;

  T getFromRoot(byte[] key) throws InvalidProtocolBufferException, ItemNotFoundException,
      BadItemException;

  T getUnchecked(byte[] key);

  boolean has(byte[] key);

  boolean isNotEmpty();

  String getName();

  String getDbName();

}
