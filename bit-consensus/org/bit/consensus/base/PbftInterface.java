package org.bit.consensus.base;

import org.bit.consensus.pbft.message.PbftBaseMessage;
import org.bit.core.capsule.BlockCapsule;

public interface PbftInterface {

  boolean isSyncing();

  void forwardMessage(PbftBaseMessage message);

  BlockCapsule getBlock(long blockNum) throws Exception;

}