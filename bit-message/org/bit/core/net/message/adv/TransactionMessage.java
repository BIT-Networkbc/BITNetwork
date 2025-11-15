package org.bit.core.net.message.adv;

import org.bit.common.overlay.message.Message;
import org.bit.common.utils.Sha256Hash;
import org.bit.core.capsule.TransactionCapsule;
import org.bit.core.net.message.MessageTypes;
import org.bit.core.net.message.BitMessage;
import org.bit.protos.Protocol.Transaction;

public class TransactionMessage extends BitMessage {

  private TransactionCapsule transactionCapsule;

  public TransactionMessage(byte[] data) throws Exception {
    super(data);
    this.transactionCapsule = new TransactionCapsule(getCodedInputStream(data));
    this.type = MessageTypes.BIT.asByte();
    if (Message.isFilter()) {
      compareBytes(data, transactionCapsule.getInstance().toByteArray());
      transactionCapsule
          .validContractProto(transactionCapsule.getInstance().getRawData().getContract(0));
    }
  }

  public TransactionMessage(Transaction bit) {
    this.transactionCapsule = new TransactionCapsule(bit);
    this.type = MessageTypes.BIT.asByte();
    this.data = bit.toByteArray();
  }

  @Override
  public String toString() {
    return new StringBuilder().append(super.toString())
        .append("messageId: ").append(super.getMessageId()).toString();
  }

  @Override
  public Sha256Hash getMessageId() {
    return this.transactionCapsule.getTransactionId();
  }

  @Override
  public Class<?> getAnswerMessage() {
    return null;
  }

  public TransactionCapsule getTransactionCapsule() {
    return this.transactionCapsule;
  }
}
