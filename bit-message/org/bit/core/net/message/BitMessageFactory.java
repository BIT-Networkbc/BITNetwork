package org.bit.core.net.message;

import org.apache.commons.lang3.ArrayUtils;
import org.bit.consensus.pbft.message.PbftMessage;
import org.bit.core.exception.P2pException;
import org.bit.core.metrics.MetricsKey;
import org.bit.core.metrics.MetricsUtil;
import org.bit.core.net.message.adv.BlockMessage;
import org.bit.core.net.message.adv.FetchInvDataMessage;
import org.bit.core.net.message.adv.InventoryMessage;
import org.bit.core.net.message.adv.TransactionMessage;
import org.bit.core.net.message.adv.TransactionsMessage;
import org.bit.core.net.message.base.DisconnectMessage;
import org.bit.core.net.message.handshake.HelloMessage;
import org.bit.core.net.message.keepalive.PingMessage;
import org.bit.core.net.message.keepalive.PongMessage;
import org.bit.core.net.message.pbft.PbftCommitMessage;
import org.bit.core.net.message.sync.ChainInventoryMessage;
import org.bit.core.net.message.sync.SyncBlockChainMessage;

public class BitMessageFactory {

  private static final String DATA_LEN = ", len=";

  public static BitMessage create(byte[] data) throws Exception {
    boolean isException = false;
    try {
      byte type = data[0];
      byte[] rawData = ArrayUtils.subarray(data, 1, data.length);
      return create(type, rawData);
    } catch (final P2pException e) {
      isException = true;
      throw e;
    } catch (final Exception e) {
      isException = true;
      throw new P2pException(P2pException.TypeEnum.PARSE_MESSAGE_FAILED,
          "type=" + data[0] + DATA_LEN + data.length + ", error msg: " + e.getMessage());
    } finally {
      if (isException) {
        MetricsUtil.counterInc(MetricsKey.NET_ERROR_PROTO_COUNT);
      }
    }
  }

  private static BitMessage create(byte type, byte[] packed) throws Exception {
    MessageTypes receivedTypes = MessageTypes.fromByte(type);
    if (receivedTypes == null) {
      throw new P2pException(P2pException.TypeEnum.NO_SUCH_MESSAGE,
          "type=" + type + DATA_LEN + packed.length);
    }
    switch (receivedTypes) {
      case P2P_HELLO:
        return new HelloMessage(packed);
      case P2P_DISCONNECT:
        return new DisconnectMessage(packed);
      case P2P_PING:
        return new PingMessage(packed);
      case P2P_PONG:
        return new PongMessage(packed);
      case BIT:
        return new TransactionMessage(packed);
      case BLOCK:
        return new BlockMessage(packed);
      case BITS:
        return new TransactionsMessage(packed);
      case INVENTORY:
        return new InventoryMessage(packed);
      case FETCH_INV_DATA:
        return new FetchInvDataMessage(packed);
      case SYNC_BLOCK_CHAIN:
        return new SyncBlockChainMessage(packed);
      case BLOCK_CHAIN_INVENTORY:
        return new ChainInventoryMessage(packed);
      case PBFT_COMMIT_MSG:
        return new PbftCommitMessage(packed);
      default:
        throw new P2pException(P2pException.TypeEnum.NO_SUCH_MESSAGE,
            receivedTypes.toString() + DATA_LEN + packed.length);
    }
  }
}
