package org.bit.core.net.message.adv;

import java.util.List;
import org.bit.core.capsule.TransactionCapsule;
import org.bit.core.net.message.MessageTypes;
import org.bit.core.net.message.BitMessage;
import org.bit.protos.Protocol;
import org.bit.protos.Protocol.Transaction;

public class TransactionsMessage extends BitMessage {

  private Protocol.Transactions transactions;

  public TransactionsMessage(List<Transaction> bits) {
    Protocol.Transactions.Builder builder = Protocol.Transactions.newBuilder();
    bits.forEach(bit -> builder.addTransactions(bit));
    this.transactions = builder.build();
    this.type = MessageTypes.BITS.asByte();
    this.data = this.transactions.toByteArray();
  }

  public TransactionsMessage(byte[] data) throws Exception {
    super(data);
    this.type = MessageTypes.BITS.asByte();
    this.transactions = Protocol.Transactions.parseFrom(getCodedInputStream(data));
    if (isFilter()) {
      compareBytes(data, transactions.toByteArray());
      TransactionCapsule.validContractProto(transactions.getTransactionsList());
    }
  }

  public Protocol.Transactions getTransactions() {
    return transactions;
  }

  @Override
  public String toString() {
    return new StringBuilder().append(super.toString()).append("bit size: ")
        .append(this.transactions.getTransactionsList().size()).toString();
  }

  @Override
  public Class<?> getAnswerMessage() {
    return null;
  }

}
