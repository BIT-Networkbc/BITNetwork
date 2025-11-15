package org.bit.core.services.jsonrpc.filters;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import lombok.Getter;
import org.bit.core.services.jsonrpc.BitJsonRpcImpl;

public abstract class FilterResult<T> {

  private long expireTimeStamp;

  @Getter
  protected BlockingQueue<T> result;

  public void updateExpireTime() {
    expireTimeStamp = System.currentTimeMillis() + BitJsonRpcImpl.EXPIRE_SECONDS * 1000;
  }

  public boolean isExpire() {
    return expireTimeStamp < System.currentTimeMillis();
  }

  public abstract void add(T t);

  public abstract List<T> popAll();
}
