package org.bit.core.net.service.statistics;

import org.bit.common.overlay.message.Message;
import org.bit.core.net.message.MessageTypes;
import org.bit.core.net.message.adv.FetchInvDataMessage;
import org.bit.core.net.message.adv.InventoryMessage;
import org.bit.core.net.message.adv.TransactionsMessage;

public class MessageStatistics {

  public final MessageCount p2pInHello = new MessageCount();
  public final MessageCount p2pOutHello = new MessageCount();
  public final MessageCount p2pInPing = new MessageCount();
  public final MessageCount p2pOutPing = new MessageCount();
  public final MessageCount p2pInPong = new MessageCount();
  public final MessageCount p2pOutPong = new MessageCount();
  public final MessageCount p2pInDisconnect = new MessageCount();
  public final MessageCount p2pOutDisconnect = new MessageCount();

  public final MessageCount bitInMessage = new MessageCount();
  public final MessageCount bitOutMessage = new MessageCount();

  public final MessageCount bitInSyncBlockChain = new MessageCount();
  public final MessageCount bitOutSyncBlockChain = new MessageCount();
  public final MessageCount bitInBlockChainInventory = new MessageCount();
  public final MessageCount bitOutBlockChainInventory = new MessageCount();

  public final MessageCount bitInBitInventory = new MessageCount();
  public final MessageCount bitOutBitInventory = new MessageCount();
  public final MessageCount bitInBitInventoryElement = new MessageCount();
  public final MessageCount bitOutBitInventoryElement = new MessageCount();

  public final MessageCount bitInBlockInventory = new MessageCount();
  public final MessageCount bitOutBlockInventory = new MessageCount();
  public final MessageCount bitInBlockInventoryElement = new MessageCount();
  public final MessageCount bitOutBlockInventoryElement = new MessageCount();

  public final MessageCount bitInBitFetchInvData = new MessageCount();
  public final MessageCount bitOutBitFetchInvData = new MessageCount();
  public final MessageCount bitInBitFetchInvDataElement = new MessageCount();
  public final MessageCount bitOutBitFetchInvDataElement = new MessageCount();

  public final MessageCount bitInBlockFetchInvData = new MessageCount();
  public final MessageCount bitOutBlockFetchInvData = new MessageCount();
  public final MessageCount bitInBlockFetchInvDataElement = new MessageCount();
  public final MessageCount bitOutBlockFetchInvDataElement = new MessageCount();


  public final MessageCount bitInBit = new MessageCount();
  public final MessageCount bitOutBit = new MessageCount();
  public final MessageCount bitInBits = new MessageCount();
  public final MessageCount bitOutBits = new MessageCount();
  public final MessageCount bitInBlock = new MessageCount();
  public final MessageCount bitOutBlock = new MessageCount();
  public final MessageCount bitOutAdvBlock = new MessageCount();

  public void addTcpInMessage(Message msg) {
    addTcpMessage(msg, true);
  }

  public void addTcpOutMessage(Message msg) {
    addTcpMessage(msg, false);
  }

  private void addTcpMessage(Message msg, boolean flag) {

    if (flag) {
      bitInMessage.add();
    } else {
      bitOutMessage.add();
    }

    switch (msg.getType()) {
      case P2P_HELLO:
        if (flag) {
          p2pInHello.add();
        } else {
          p2pOutHello.add();
        }
        break;
      case P2P_PING:
        if (flag) {
          p2pInPing.add();
        } else {
          p2pOutPing.add();
        }
        break;
      case P2P_PONG:
        if (flag) {
          p2pInPong.add();
        } else {
          p2pOutPong.add();
        }
        break;
      case P2P_DISCONNECT:
        if (flag) {
          p2pInDisconnect.add();
        } else {
          p2pOutDisconnect.add();
        }
        break;
      case SYNC_BLOCK_CHAIN:
        if (flag) {
          bitInSyncBlockChain.add();
        } else {
          bitOutSyncBlockChain.add();
        }
        break;
      case BLOCK_CHAIN_INVENTORY:
        if (flag) {
          bitInBlockChainInventory.add();
        } else {
          bitOutBlockChainInventory.add();
        }
        break;
      case INVENTORY:
        InventoryMessage inventoryMessage = (InventoryMessage) msg;
        int inventorySize = inventoryMessage.getInventory().getIdsCount();
        messageProcess(inventoryMessage.getInvMessageType(),
                bitInBitInventory,bitInBitInventoryElement,bitInBlockInventory,
                bitInBlockInventoryElement,bitOutBitInventory,bitOutBitInventoryElement,
                bitOutBlockInventory,bitOutBlockInventoryElement,
                flag, inventorySize);
        break;
      case FETCH_INV_DATA:
        FetchInvDataMessage fetchInvDataMessage = (FetchInvDataMessage) msg;
        int fetchSize = fetchInvDataMessage.getInventory().getIdsCount();
        messageProcess(fetchInvDataMessage.getInvMessageType(),
                bitInBitFetchInvData,bitInBitFetchInvDataElement,bitInBlockFetchInvData,
                bitInBlockFetchInvDataElement,bitOutBitFetchInvData,bitOutBitFetchInvDataElement,
                bitOutBlockFetchInvData,bitOutBlockFetchInvDataElement,
                flag, fetchSize);
        break;
      case BITS:
        TransactionsMessage transactionsMessage = (TransactionsMessage) msg;
        if (flag) {
          bitInBits.add();
          bitInBit.add(transactionsMessage.getTransactions().getTransactionsCount());
        } else {
          bitOutBits.add();
          bitOutBit.add(transactionsMessage.getTransactions().getTransactionsCount());
        }
        break;
      case BIT:
        if (flag) {
          bitInBit.add();
        } else {
          bitOutBit.add();
        }
        break;
      case BLOCK:
        if (flag) {
          bitInBlock.add();
        } else {
          bitOutBlock.add();
        }
        break;
      default:
        break;
    }
  }


  private void messageProcess(MessageTypes messageType,
                              MessageCount inBit,
                              MessageCount inBitEle,
                              MessageCount inBlock,
                              MessageCount inBlockEle,
                              MessageCount outBit,
                              MessageCount outBitEle,
                              MessageCount outBlock,
                              MessageCount outBlockEle,
                              boolean flag, int size) {
    if (flag) {
      if (messageType == MessageTypes.BIT) {
        inBit.add();
        inBitEle.add(size);
      } else {
        inBlock.add();
        inBlockEle.add(size);
      }
    } else {
      if (messageType == MessageTypes.BIT) {
        outBit.add();
        outBitEle.add(size);
      } else {
        outBlock.add();
        outBlockEle.add(size);
      }
    }
  }

}
