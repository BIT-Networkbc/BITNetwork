package org.bit.core.net.message.adv;

import org.bit.common.overlay.message.Message;
import org.bit.common.utils.Sha256Hash;
import org.bit.core.capsule.BlockCapsule;
import org.bit.core.capsule.BlockCapsule.BlockId;
import org.bit.core.capsule.TransactionCapsule;
import org.bit.core.net.message.MessageTypes;
import org.bit.core.net.message.BitMessage;

public class BlockMessage extends BitMessage {

  private BlockCapsule block;

  public BlockMessage(byte[] data) throws Exception {
    super(data);
    this.type = MessageTypes.BLOCK.asByte();
    this.block = new BlockCapsule(getCodedInputStream(data));
    if (Message.isFilter()) {
      Message.compareBytes(data, block.getInstance().toByteArray());
      TransactionCapsule.validContractProto(block.getInstance().getTransactionsList());
    }
  }

  public BlockMessage(BlockCapsule block) {
    data = block.getData();
    this.type = MessageTypes.BLOCK.asByte();
    this.block = block;
  }

  public BlockId getBlockId() {
    return getBlockCapsule().getBlockId();
  }

  public BlockCapsule getBlockCapsule() {
    return block;
  }

  @Override
  public Class<?> getAnswerMessage() {
    return null;
  }

  @Override
  public Sha256Hash getMessageId() {
    return getBlockCapsule().getBlockId();
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public String toString() {
    return new StringBuilder().append(super.toString()).append(block.getBlockId().getString())
        .append(", bit size: ").append(block.getTransactions().size()).append("\n").toString();
  }
}
