package org.bit.core.db;

import lombok.Data;
import org.bit.common.runtime.ProgramResult;
import org.bit.core.capsule.BlockCapsule;
import org.bit.core.capsule.TransactionCapsule;
import org.bit.core.store.StoreFactory;

@Data
public class TransactionContext {

  private BlockCapsule blockCap;
  private TransactionCapsule bitCap;
  private StoreFactory storeFactory;
  private ProgramResult programResult = new ProgramResult();
  private boolean isStatic;
  private boolean eventPluginLoaded;

  public TransactionContext(BlockCapsule blockCap, TransactionCapsule bitCap,
      StoreFactory storeFactory,
      boolean isStatic,
      boolean eventPluginLoaded) {
    this.blockCap = blockCap;
    this.bitCap = bitCap;
    this.storeFactory = storeFactory;
    this.isStatic = isStatic;
    this.eventPluginLoaded = eventPluginLoaded;
  }
}
