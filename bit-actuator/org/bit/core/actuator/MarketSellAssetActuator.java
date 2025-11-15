/*
 * java-bit is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * java-bit is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.bit.core.actuator;

import static org.bit.core.actuator.ActuatorConstant.CONTRACT_NOT_EXIST;
import static org.bit.core.actuator.ActuatorConstant.STORE_NOT_EXIST;
import static org.bit.core.actuator.ActuatorConstant.TX_RESULT_NULL;
import static org.bit.core.capsule.utils.TransactionUtil.isNumber;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.bit.common.utils.BigIntegerUtil;
import org.bit.common.utils.Commons;
import org.bit.common.utils.DecodeUtil;
import org.bit.core.capsule.AccountCapsule;
import org.bit.core.capsule.AssetIssueCapsule;
import org.bit.core.capsule.MarketAccountOrderCapsule;
import org.bit.core.capsule.MarketOrderCapsule;
import org.bit.core.capsule.MarketOrderIdListCapsule;
import org.bit.core.capsule.TransactionResultCapsule;
import org.bit.core.capsule.utils.MarketUtils;
import org.bit.core.exception.BalanceInsufficientException;
import org.bit.core.exception.ContractExeException;
import org.bit.core.exception.ContractValidateException;
import org.bit.core.exception.ItemNotFoundException;
import org.bit.core.store.AccountStore;
import org.bit.core.store.AssetIssueStore;
import org.bit.core.store.AssetIssueV2Store;
import org.bit.core.store.DynamicPropertiesStore;
import org.bit.core.store.MarketAccountStore;
import org.bit.core.store.MarketOrderStore;
import org.bit.core.store.MarketPairPriceToOrderStore;
import org.bit.core.store.MarketPairToPriceStore;
import org.bit.protos.Protocol.MarketOrder.State;
import org.bit.protos.Protocol.MarketOrderDetail;
import org.bit.protos.Protocol.MarketPrice;
import org.bit.protos.Protocol.Transaction.Contract.ContractType;
import org.bit.protos.Protocol.Transaction.Result.code;
import org.bit.protos.contract.AssetIssueContractOuterClass.AssetIssueContract;
import org.bit.protos.contract.MarketContract.MarketSellAssetContract;

@Slf4j(topic = "actuator")
public class MarketSellAssetActuator extends AbstractActuator {

  @Getter
  @Setter
  private static int MAX_ACTIVE_ORDER_NUM = 100;
  @Getter
  private static int MAX_MATCH_NUM = 20;

  private AccountStore accountStore;
  private DynamicPropertiesStore dynamicStore;
  private AssetIssueStore assetIssueStore;
  private AssetIssueV2Store assetIssueV2Store;

  private MarketAccountStore marketAccountStore;
  private MarketOrderStore orderStore;
  private MarketPairToPriceStore pairToPriceStore;
  private MarketPairPriceToOrderStore pairPriceToOrderStore;

  private byte[] sellTokenID = null;
  private byte[] buyTokenID = null;
  private BigInteger sellTokenQuantity;
  private BigInteger buyTokenQuantity;

  public MarketSellAssetActuator() {
    super(ContractType.MarketSellAssetContract, MarketSellAssetContract.class);
  }

  private void initStores() {
    accountStore = chainBaseManager.getAccountStore();
    dynamicStore = chainBaseManager.getDynamicPropertiesStore();
    assetIssueStore = chainBaseManager.getAssetIssueStore();
    assetIssueV2Store = chainBaseManager.getAssetIssueV2Store();

    marketAccountStore = chainBaseManager.getMarketAccountStore();
    orderStore = chainBaseManager.getMarketOrderStore();
    pairToPriceStore = chainBaseManager.getMarketPairToPriceStore();
    pairPriceToOrderStore = chainBaseManager.getMarketPairPriceToOrderStore();
  }

  @Override
  public boolean execute(Object object) throws ContractExeException {
    initStores();

    TransactionResultCapsule ret = (TransactionResultCapsule) object;
    if (Objects.isNull(ret)) {
      throw new RuntimeException(TX_RESULT_NULL);
    }

    long fee = calcFee();

    try {
      final MarketSellAssetContract contract = this.any
          .unpack(MarketSellAssetContract.class);

      AccountCapsule accountCapsule = accountStore
          .get(contract.getOwnerAddress().toByteArray());

      sellTokenID = contract.getSellTokenId().toByteArray();
      buyTokenID = contract.getBuyTokenId().toByteArray();
      sellTokenQuantity = BigIntegerUtil.newInstance(contract.getSellTokenQuantity());
      buyTokenQuantity = BigIntegerUtil.newInstance(contract.getBuyTokenQuantity());
      MarketPrice takerPrice = MarketPrice.newBuilder()
          .setSellTokenQuantity(sellTokenQuantity.toString())
          .setBuyTokenQuantity(buyTokenQuantity.toString()).build();

      // fee
      accountCapsule.setBalance(accountCapsule.getBalance().subtract(BigInteger.valueOf(fee)).toString());
      // add to blackhole address
      if (dynamicStore.supportBlackHoleOptimization()) {
        dynamicStore.burnBit(fee);
      } else {
        Commons.adjustBalance(accountStore, accountStore.getBlackhole(), BigInteger.valueOf(fee));
      }
      // 1. transfer of balance
      transferBalanceOrToken(accountCapsule);

      // 2. create and save order
      MarketOrderCapsule orderCapsule = createAndSaveOrder(accountCapsule, contract);

      // 3. match order
      matchOrder(orderCapsule, takerPrice, ret, accountCapsule);

      // 4. save remain order into order book
      if (orderCapsule.getSellTokenQuantityRemain().compareTo(BigInteger.ZERO) != 0) {
        saveRemainOrder(orderCapsule);
      }

      orderStore.put(orderCapsule.getID().toByteArray(), orderCapsule);
      accountStore.put(accountCapsule.createDbKey(), accountCapsule);

      ret.setOrderId(orderCapsule.getID());
      ret.setStatus(fee, code.SUCESS);
    } catch (ItemNotFoundException
        | InvalidProtocolBufferException
        | BalanceInsufficientException
        | ContractValidateException e) {
      logger.debug(e.getMessage(), e);
      ret.setStatus(fee, code.FAILED);
      throw new ContractExeException(e.getMessage());
    }

    return true;
  }

  @Override
  public boolean validate() throws ContractValidateException {
    if (this.any == null) {
      throw new ContractValidateException(CONTRACT_NOT_EXIST);
    }
    if (chainBaseManager == null) {
      throw new ContractValidateException(STORE_NOT_EXIST);
    }

    initStores();

    if (!this.any.is(MarketSellAssetContract.class)) {
      throw new ContractValidateException(
          "contract type error,expected type [MarketSellAssetContract],real type[" + any
              .getClass() + "]");
    }

    if (!dynamicStore.supportAllowMarketTransaction()) {
      throw new ContractValidateException("Not support Market Transaction, need to be opened by"
          + " the committee");
    }

    final MarketSellAssetContract contract;
    try {
      contract =
          this.any.unpack(MarketSellAssetContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }

    // Parameters check
    byte[] ownerAddress = contract.getOwnerAddress().toByteArray();
    sellTokenID = contract.getSellTokenId().toByteArray();
    buyTokenID = contract.getBuyTokenId().toByteArray();
    sellTokenQuantity = BigIntegerUtil.newInstance(contract.getSellTokenQuantity());
    buyTokenQuantity = BigIntegerUtil.newInstance(contract.getBuyTokenQuantity());

    if (!DecodeUtil.addressValid(ownerAddress)) {
      throw new ContractValidateException("Invalid address");
    }

    // Whether the accountStore exist
    AccountCapsule ownerAccount = accountStore.get(ownerAddress);
    if (ownerAccount == null) {
      throw new ContractValidateException("Account does not exist!");
    }

    if (!Arrays.equals(sellTokenID, "_".getBytes()) && !isNumber(sellTokenID)) {
      throw new ContractValidateException("sellTokenId is not a valid number");
    }
    if (!Arrays.equals(buyTokenID, "_".getBytes()) && !isNumber(buyTokenID)) {
      throw new ContractValidateException("buyTokenId is not a valid number");
    }

    if (Arrays.equals(sellTokenID, buyTokenID)) {
      throw new ContractValidateException("cannot exchange same tokens");
    }

    if (sellTokenQuantity.compareTo(BigInteger.ZERO) <= 0 || buyTokenQuantity.compareTo(BigInteger.ZERO) <= 0) {
      throw new ContractValidateException("token quantity must greater than zero");
    }

    BigInteger quantityLimit = dynamicStore.getMarketQuantityLimit();
    if (sellTokenQuantity.compareTo(quantityLimit) > 0 || buyTokenQuantity.compareTo(quantityLimit) > 0) {
      throw new ContractValidateException("token quantity must less than " + quantityLimit);
    }

    // check order num
    MarketAccountOrderCapsule marketAccountOrderCapsule = marketAccountStore
        .getUnchecked(ownerAddress);
    if (marketAccountOrderCapsule != null
        && marketAccountOrderCapsule.getCount() >= MAX_ACTIVE_ORDER_NUM) {
      throw new ContractValidateException(
          "Maximum number of orders exceeded，" + MAX_ACTIVE_ORDER_NUM);
    }

    try {
      // Whether the balance is enough
      long fee = calcFee();

      if (Arrays.equals(sellTokenID, "_".getBytes())) {
        if (ownerAccount.getBalance().compareTo(sellTokenQuantity.add(BigInteger.valueOf(fee))) < 0) {
          throw new ContractValidateException("No enough balance !");
        }
      } else {
        if (ownerAccount.getBalance().compareTo(BigInteger.valueOf(fee)) < 0) {
          throw new ContractValidateException("No enough balance !");
        }

        AssetIssueCapsule assetIssueCapsule = Commons
            .getAssetIssueStoreFinal(dynamicStore, assetIssueStore, assetIssueV2Store)
            .get(sellTokenID);
        if (assetIssueCapsule == null) {
          throw new ContractValidateException("No sellTokenId !");
        }
        if (!ownerAccount.assetBalanceEnoughV2(sellTokenID, sellTokenQuantity,
            dynamicStore)) {
          throw new ContractValidateException("SellToken balance is not enough !");
        }
      }

      if (!Arrays.equals(buyTokenID, "_".getBytes())) {
        // Whether have the token
        AssetIssueCapsule assetIssueCapsule = Commons
            .getAssetIssueStoreFinal(dynamicStore, assetIssueStore, assetIssueV2Store)
            .get(buyTokenID);
        if (assetIssueCapsule == null) {
          throw new ContractValidateException("No buyTokenId !");
        }
      }
    } catch (ArithmeticException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }

    return true;
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return any.unpack(AssetIssueContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return dynamicStore.getMarketSellFee();
  }

  /**
   * return marketPrice if matched, otherwise null
   */
  private MarketPrice hasMatch(List<byte[]> priceKeysList, MarketPrice takerPrice) {
    if (priceKeysList.isEmpty()) {
      return null;
    }

    // get the first key which is the lowest price
    MarketPrice bestPrice = MarketUtils.decodeKeyToMarketPrice(priceKeysList.get(0));

    return MarketUtils.priceMatch(takerPrice, bestPrice) ? bestPrice : null;
  }

  private void matchOrder(MarketOrderCapsule takerCapsule, MarketPrice takerPrice,
      TransactionResultCapsule ret, AccountCapsule takerAccountCapsule)
      throws ItemNotFoundException, ContractValidateException {

    byte[] makerSellTokenID = buyTokenID;
    byte[] makerBuyTokenID = sellTokenID;
    byte[] makerPair = MarketUtils.createPairKey(makerSellTokenID, makerBuyTokenID);

    // makerPair not exists
    long makerPriceNumber = pairToPriceStore.getPriceNum(makerPair);
    if (makerPriceNumber == 0) {
      return;
    }
    long remainCount = makerPriceNumber;

    // get maker price list
    List<byte[]> priceKeysList = pairPriceToOrderStore
        .getPriceKeysList(MarketUtils.getPairPriceHeadKey(makerSellTokenID, makerBuyTokenID),
            (long) (MAX_MATCH_NUM + 1), makerPriceNumber, true);

    int matchOrderCount = 0;
    // match different price
    while (takerCapsule.getSellTokenQuantityRemain().compareTo(BigInteger.ZERO) != 0) {
      // get lowest ordersList
      MarketPrice makerPrice = hasMatch(priceKeysList, takerPrice);
      if (makerPrice == null) {
        return;
      }

      byte[] pairPriceKey = priceKeysList.get(0);

      // if not exists
      MarketOrderIdListCapsule orderIdListCapsule = pairPriceToOrderStore.get(pairPriceKey);

      // match different orders which have the same price
      while (takerCapsule.getSellTokenQuantityRemain().compareTo(BigInteger.ZERO) != 0
          && !orderIdListCapsule.isOrderEmpty()) {
        byte[] orderId = orderIdListCapsule.getHead();
        MarketOrderCapsule makerOrderCapsule = orderStore.get(orderId);

        matchSingleOrder(takerCapsule, makerOrderCapsule, ret, takerAccountCapsule);

        // remove order
        if (makerOrderCapsule.getSellTokenQuantityRemain().compareTo(BigInteger.ZERO) == 0) {
          // remove from market order list
          orderIdListCapsule.removeOrder(makerOrderCapsule, orderStore,
              pairPriceKey, pairPriceToOrderStore);
        }

        matchOrderCount++;
        if (matchOrderCount > MAX_MATCH_NUM) {
          throw new ContractValidateException("Too many matches. MAX_MATCH_NUM = " + MAX_MATCH_NUM);
        }
      }

      // the orders of makerPrice have been all consumed
      if (orderIdListCapsule.isOrderEmpty()) {
        pairPriceToOrderStore.delete(pairPriceKey);

        // need to delete marketPair if no more price(priceKeysList is empty after deleting)
        priceKeysList.remove(0);

        // update priceInfo's count
        remainCount = remainCount - 1;
        // if really empty, need to delete token pair from pairToPriceStore
        if (remainCount == 0) {
          pairToPriceStore.delete(makerPair);
          break;
        } else {
          pairToPriceStore.setPriceNum(makerPair, remainCount);
        }
      }
    } // end while
  }

  // return all match or not
  private void matchSingleOrder(MarketOrderCapsule takerOrderCapsule,
      MarketOrderCapsule makerOrderCapsule, TransactionResultCapsule ret,
      AccountCapsule takerAccountCapsule)
      throws ItemNotFoundException {

    BigInteger takerSellRemainQuantity = takerOrderCapsule.getSellTokenQuantityRemain();
    BigInteger makerSellQuantity = makerOrderCapsule.getSellTokenQuantity();
    BigInteger makerBuyQuantity = makerOrderCapsule.getBuyTokenQuantity();
    BigInteger makerSellRemainQuantity = makerOrderCapsule.getSellTokenQuantityRemain();

    // according to the price of maker, calculate the quantity of taker can buy
    // for makerPrice,sellToken is A,buyToken is BIT.
    // for takerPrice,buyToken is A,sellToken is BIT.

    // makerSellTokenQuantity_A/makerBuyTokenQuantity_BIT =
    //   takerBuyTokenQuantityCurrent_A/takerSellTokenQuantityRemain_BIT
    // => takerBuyTokenQuantityCurrent_A = takerSellTokenQuantityRemain_BIT *
    //   makerSellTokenQuantity_A/makerBuyTokenQuantity_BIT

    BigInteger takerBuyTokenQuantityRemain = MarketUtils
        .multiplyAndDivide(takerSellRemainQuantity, makerSellQuantity, makerBuyQuantity);

    if (takerBuyTokenQuantityRemain.compareTo(BigInteger.ZERO) == 0) {
      // quantity too small, return sellToken to user
      takerOrderCapsule.setSellTokenQuantityReturn();
      MarketUtils.returnSellTokenRemain(takerOrderCapsule, takerAccountCapsule,
          dynamicStore, assetIssueStore);
      MarketUtils.updateOrderState(takerOrderCapsule, State.INACTIVE, marketAccountStore);
      return;
    }

    BigInteger takerBuyTokenQuantityReceive; // In this match, the token obtained by taker
    BigInteger makerBuyTokenQuantityReceive; // the token obtained by maker

    if (takerBuyTokenQuantityRemain.compareTo(makerOrderCapsule.getSellTokenQuantityRemain()) == 0) {
      // taker == maker

      // makerSellTokenQuantityRemain_A/makerBuyTokenQuantityCurrent_BIT =
      //   makerSellTokenQuantity_A/makerBuyTokenQuantity_BIT
      // => makerBuyTokenQuantityCurrent_BIT = makerSellTokenQuantityRemain_A *
      //   makerBuyTokenQuantity_BIT / makerSellTokenQuantity_A

      makerBuyTokenQuantityReceive = MarketUtils
          .multiplyAndDivide(makerSellRemainQuantity, makerBuyQuantity, makerSellQuantity);
      takerBuyTokenQuantityReceive = makerOrderCapsule.getSellTokenQuantityRemain();

      BigInteger takerSellTokenLeft =
          takerOrderCapsule.getSellTokenQuantityRemain().subtract(makerBuyTokenQuantityReceive);
      takerOrderCapsule.setSellTokenQuantityRemain(takerSellTokenLeft.toString());
      makerOrderCapsule.setSellTokenQuantityRemain("0");

      if (takerSellTokenLeft.compareTo(BigInteger.ZERO) == 0) {
        MarketUtils.updateOrderState(takerOrderCapsule, State.INACTIVE, marketAccountStore);
      }
      MarketUtils.updateOrderState(makerOrderCapsule, State.INACTIVE, marketAccountStore);
    } else if (takerBuyTokenQuantityRemain.compareTo(makerOrderCapsule.getSellTokenQuantityRemain()) < 0) {
      // taker < maker
      // if the quantity of taker want to buy is smaller than the remain of maker want to sell,
      // consume the order of the taker

      takerBuyTokenQuantityReceive = takerBuyTokenQuantityRemain;
      makerBuyTokenQuantityReceive = takerOrderCapsule.getSellTokenQuantityRemain();

      takerOrderCapsule.setSellTokenQuantityRemain("0");
      MarketUtils.updateOrderState(takerOrderCapsule, State.INACTIVE, marketAccountStore);

      makerOrderCapsule.setSellTokenQuantityRemain(makerOrderCapsule.getSellTokenQuantityRemain().subtract(takerBuyTokenQuantityRemain).toString());
    } else {
      // taker > maker
      takerBuyTokenQuantityReceive = makerOrderCapsule.getSellTokenQuantityRemain();

      // if the quantity of taker want to buy is bigger than the remain of maker want to sell,
      // consume the order of maker
      // makerSellTokenQuantityRemain_A/makerBuyTokenQuantityCurrent_BIT =
      //   makerSellTokenQuantity_A/makerBuyTokenQuantity_BIT
      makerBuyTokenQuantityReceive = MarketUtils
          .multiplyAndDivide(makerSellRemainQuantity, makerBuyQuantity, makerSellQuantity);

      MarketUtils.updateOrderState(makerOrderCapsule, State.INACTIVE, marketAccountStore);
      if (makerBuyTokenQuantityReceive.compareTo(BigInteger.ZERO) == 0) {
        // the quantity is too small, return the remain of sellToken to maker
        // it would not happen here
        // for the maker, when sellQuantity < buyQuantity, it will get at least one buyToken
        // even when sellRemain = 1.
        // so if sellQuantity=200，buyQuantity=100, when sellRemain=1, it needs to be satisfied
        // the following conditions:
        // makerOrderCapsule.getSellTokenQuantityRemain() - takerBuyTokenQuantityRemain = 1
        // 200 - 200/100 * X = 1 ===> X = 199/2，and this comports with the fact that X is integer.
        makerOrderCapsule.setSellTokenQuantityReturn();
        returnSellTokenRemain(makerOrderCapsule);
        return;
      } else {
        makerOrderCapsule.setSellTokenQuantityRemain("0");
        takerOrderCapsule.setSellTokenQuantityRemain(takerOrderCapsule.getSellTokenQuantityRemain().subtract(makerBuyTokenQuantityReceive).toString());
      }
    }

    // save makerOrderCapsule
    orderStore.put(makerOrderCapsule.getID().toByteArray(), makerOrderCapsule);

    // add token into account
    addBitOrToken(takerOrderCapsule, takerBuyTokenQuantityReceive, takerAccountCapsule);
    addBitOrToken(makerOrderCapsule, makerBuyTokenQuantityReceive);

    MarketOrderDetail orderDetail = MarketOrderDetail.newBuilder()
        .setMakerOrderId(makerOrderCapsule.getID())
        .setTakerOrderId(takerOrderCapsule.getID())
        .setFillSellQuantity(makerBuyTokenQuantityReceive.toString())
        .setFillBuyQuantity(takerBuyTokenQuantityReceive.toString())
        .build();
    ret.addOrderDetails(orderDetail);
  }

  private MarketOrderCapsule createAndSaveOrder(AccountCapsule accountCapsule,
      MarketSellAssetContract contract) {
    MarketAccountOrderCapsule marketAccountOrderCapsule = marketAccountStore
        .getUnchecked(contract.getOwnerAddress().toByteArray());
    if (marketAccountOrderCapsule == null) {
      marketAccountOrderCapsule = new MarketAccountOrderCapsule(contract.getOwnerAddress());
    }

    // note: here use total_count
    byte[] orderId = MarketUtils
        .calculateOrderId(contract.getOwnerAddress(), sellTokenID, buyTokenID,
            marketAccountOrderCapsule.getTotalCount());
    MarketOrderCapsule orderCapsule = new MarketOrderCapsule(orderId, contract);

    long now = dynamicStore.getLatestBlockHeaderTimestamp();
    orderCapsule.setCreateTime(now);

    marketAccountOrderCapsule.addOrders(orderCapsule.getID());
    marketAccountOrderCapsule.setCount(marketAccountOrderCapsule.getCount() + 1);
    marketAccountOrderCapsule.setTotalCount(marketAccountOrderCapsule.getTotalCount() + 1);
    marketAccountStore.put(accountCapsule.createDbKey(), marketAccountOrderCapsule);
    orderStore.put(orderId, orderCapsule);

    return orderCapsule;
  }

  private void transferBalanceOrToken(AccountCapsule accountCapsule) {
    if (Arrays.equals(sellTokenID, "_".getBytes())) {
      accountCapsule.setBalance(accountCapsule.getBalance().subtract(sellTokenQuantity).toString());
    } else {
      accountCapsule
          .reduceAssetAmountV2(sellTokenID, sellTokenQuantity, dynamicStore, assetIssueStore);
    }
  }

  // for taker
  private void addBitOrToken(MarketOrderCapsule orderCapsule, BigInteger num,
      AccountCapsule accountCapsule) {

    byte[] buyTokenId = orderCapsule.getBuyTokenId();
    if (Arrays.equals(buyTokenId, "_".getBytes())) {
      accountCapsule.setBalance(accountCapsule.getBalance().add(num).toString());
    } else {
      accountCapsule
          .addAssetAmountV2(buyTokenId, num, dynamicStore, assetIssueStore);
    }
  }

  private void addBitOrToken(MarketOrderCapsule orderCapsule, BigInteger num) {
    AccountCapsule accountCapsule = accountStore
        .get(orderCapsule.getOwnerAddress().toByteArray());

    byte[] buyTokenId = orderCapsule.getBuyTokenId();
    if (Arrays.equals(buyTokenId, "_".getBytes())) {
      accountCapsule.setBalance(accountCapsule.getBalance().add(num).toString());
    } else {
      accountCapsule
          .addAssetAmountV2(buyTokenId, num, dynamicStore, assetIssueStore);
    }
    accountStore.put(orderCapsule.getOwnerAddress().toByteArray(), accountCapsule);
  }

  private void returnSellTokenRemain(MarketOrderCapsule orderCapsule) {
    AccountCapsule accountCapsule = accountStore
        .get(orderCapsule.getOwnerAddress().toByteArray());

    MarketUtils.returnSellTokenRemain(orderCapsule, accountCapsule, dynamicStore, assetIssueStore);
    accountStore.put(orderCapsule.getOwnerAddress().toByteArray(), accountCapsule);
  }

  private void saveRemainOrder(MarketOrderCapsule orderCapsule)
      throws ItemNotFoundException {
    // add order into orderList
    byte[] pairPriceKey = MarketUtils.createPairPriceKey(
        sellTokenID,
        buyTokenID,
        sellTokenQuantity,
        buyTokenQuantity
    );

    MarketOrderIdListCapsule orderIdListCapsule = pairPriceToOrderStore.getUnchecked(pairPriceKey);
    if (orderIdListCapsule == null) {
      orderIdListCapsule = new MarketOrderIdListCapsule();

      // pairPriceKey not exists, increase price count:
      // if pair not exits, add token pair, set count = 1, add headKey to pairPriceToOrderStore
      // if pair exists, increase count
      pairToPriceStore.addNewPriceKey(sellTokenID, buyTokenID, pairPriceToOrderStore);
    }

    orderIdListCapsule.addOrder(orderCapsule, orderStore);
    pairPriceToOrderStore.put(pairPriceKey, orderIdListCapsule);
  }

}
