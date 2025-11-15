package org.bit.consensus.base;

import org.bit.consensus.base.Param.Miner;
import org.bit.core.capsule.BlockCapsule;

public interface BlockHandle {

  State getState();

  Object getLock();

  BlockCapsule produce(Miner miner, long blockTime, long timeout);

  void setBlockWaitLock(boolean flag);

}