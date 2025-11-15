package org.bit.core.services.jsonrpc.filters;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.Getter;
import org.bit.core.Wallet;
import org.bit.core.exception.JsonRpcInvalidParamsException;
import org.bit.core.services.jsonrpc.BitJsonRpc.FilterRequest;
import org.bit.core.services.jsonrpc.BitJsonRpc.LogFilterElement;

public class LogFilterAndResult extends FilterResult<LogFilterElement> {

  @Getter
  private final LogFilterWrapper logFilterWrapper;

  public LogFilterAndResult(FilterRequest fr, long currentMaxBlockNum, Wallet wallet)
      throws JsonRpcInvalidParamsException {
    this.logFilterWrapper = new LogFilterWrapper(fr, currentMaxBlockNum, wallet);
    result = new LinkedBlockingQueue<>();
    this.updateExpireTime();
  }

  @Override
  public void add(LogFilterElement logFilterElement) {
    result.add(logFilterElement);
  }

  @Override
  public List<LogFilterElement> popAll() {
    List<LogFilterElement> elements = new ArrayList<>();
    result.drainTo(elements);
    return elements;
  }
}
