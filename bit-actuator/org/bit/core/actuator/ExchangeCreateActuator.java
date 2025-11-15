package org.bit.core.actuator;

import static org.bit.core.actuator.ActuatorConstant.NOT_EXIST_STR;
import static org.bit.core.capsule.utils.TransactionUtil.isNumber;
import static org.bit.core.config.Parameter.ChainSymbol.BIT_SYMBOL_BYTES;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.bit.common.utils.BigIntegerUtil;
import org.bit.common.utils.Commons;
import org.bit.common.utils.DecodeUtil;
import org.bit.common.utils.StringUtil;
import org.bit.core.capsule.AccountCapsule;
import org.bit.core.capsule.ExchangeCapsule;
import org.bit.core.capsule.TransactionResultCapsule;
import org.bit.core.exception.BalanceInsufficientException;
import org.bit.core.exception.ContractExeException;
import org.bit.core.exception.ContractValidateException;
import org.bit.core.store.AccountStore;
import org.bit.core.store.AssetIssueStore;
import org.bit.core.store.DynamicPropertiesStore;
import org.bit.core.store.ExchangeStore;
import org.bit.core.store.ExchangeV2Store;
import org.bit.protos.Protocol.Transaction.Contract.ContractType;
import org.bit.protos.Protocol.Transaction.Result.code;
import org.bit.protos.contract.ExchangeContract.ExchangeCreateContract;

@Slf4j(topic = "actuator")
public class ExchangeCreateActuator extends AbstractActuator {

  public ExchangeCreateActuator() {
    super(ContractType.ExchangeCreateContract, ExchangeCreateContract.class);
  }

  @Override
  public boolean execute(Object object) throws ContractExeException {
    TransactionResultCapsule ret = (TransactionResultCapsule) object;
    if (Objects.isNull(ret)) {
      throw new RuntimeException(ActuatorConstant.TX_RESULT_NULL);
    }

    long fee = calcFee();
    AccountStore accountStore = chainBaseManager.getAccountStore();
    DynamicPropertiesStore dynamicStore = chainBaseManager.getDynamicPropertiesStore();
    AssetIssueStore assetIssueStore = chainBaseManager.getAssetIssueStore();
    ExchangeStore exchangeStore = chainBaseManager.getExchangeStore();
    ExchangeV2Store exchangeV2Store = chainBaseManager.getExchangeV2Store();
    try {
      final ExchangeCreateContract exchangeCreateContract = this.any
          .unpack(ExchangeCreateContract.class);
      AccountCapsule accountCapsule = accountStore
          .get(exchangeCreateContract.getOwnerAddress().toByteArray());

      byte[] firstTokenID = exchangeCreateContract.getFirstTokenId().toByteArray();
      byte[] secondTokenID = exchangeCreateContract.getSecondTokenId().toByteArray();
      BigInteger firstTokenBalance = BigIntegerUtil.newInstance(exchangeCreateContract.getFirstTokenBalance());
      BigInteger secondTokenBalance = BigIntegerUtil.newInstance(exchangeCreateContract.getSecondTokenBalance());

      BigInteger newBalance = accountCapsule.getBalance().subtract(BigInteger.valueOf(fee));

      accountCapsule.setBalance(newBalance.toString());

      if (Arrays.equals(firstTokenID, BIT_SYMBOL_BYTES)) {
        accountCapsule.setBalance(newBalance.subtract(firstTokenBalance).toString());
      } else {
        accountCapsule
            .reduceAssetAmountV2(firstTokenID, firstTokenBalance, dynamicStore, assetIssueStore);
      }

      if (Arrays.equals(secondTokenID, BIT_SYMBOL_BYTES)) {
        accountCapsule.setBalance(newBalance.subtract(secondTokenBalance).toString());
      } else {
        accountCapsule
            .reduceAssetAmountV2(secondTokenID, secondTokenBalance, dynamicStore, assetIssueStore);
      }

      long id = dynamicStore.getLatestExchangeNum() + 1;
      long now = dynamicStore.getLatestBlockHeaderTimestamp();
      if (dynamicStore.getAllowSameTokenName() == 0) {
        //save to old asset store
        ExchangeCapsule exchangeCapsule =
            new ExchangeCapsule(
                exchangeCreateContract.getOwnerAddress(),
                id,
                now,
                firstTokenID,
                secondTokenID
            );
        exchangeCapsule.setBalance(firstTokenBalance.toString(), secondTokenBalance.toString());
        exchangeStore.put(exchangeCapsule.createDbKey(), exchangeCapsule);

        //save to new asset store
        if (!Arrays.equals(firstTokenID, BIT_SYMBOL_BYTES)) {
          String firstTokenRealID = assetIssueStore.get(firstTokenID).getId();
          firstTokenID = firstTokenRealID.getBytes();
        }
        if (!Arrays.equals(secondTokenID, BIT_SYMBOL_BYTES)) {
          String secondTokenRealID = assetIssueStore.get(secondTokenID).getId();
          secondTokenID = secondTokenRealID.getBytes();
        }
      }

      {
        // only save to new asset store
        ExchangeCapsule exchangeCapsuleV2 =
            new ExchangeCapsule(
                exchangeCreateContract.getOwnerAddress(),
                id,
                now,
                firstTokenID,
                secondTokenID
            );
        exchangeCapsuleV2.setBalance(firstTokenBalance.toString(), secondTokenBalance.toString());
        exchangeV2Store.put(exchangeCapsuleV2.createDbKey(), exchangeCapsuleV2);
      }

      accountStore.put(accountCapsule.createDbKey(), accountCapsule);
      dynamicStore.saveLatestExchangeNum(id);
      if (dynamicStore.supportBlackHoleOptimization()) {
        dynamicStore.burnBit(fee);
      } else {
        Commons.adjustBalance(accountStore, accountStore.getBlackhole(), BigInteger.valueOf(fee));
      }
      ret.setExchangeId(id);
      ret.setStatus(fee, code.SUCESS);
    } catch (BalanceInsufficientException | InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      ret.setStatus(fee, code.FAILED);
      throw new ContractExeException(e.getMessage());
    }
    return true;
  }

  @Override
  public boolean validate() throws ContractValidateException {
    if (this.any == null) {
      throw new ContractValidateException(ActuatorConstant.CONTRACT_NOT_EXIST);
    }
    if (chainBaseManager == null) {
      throw new ContractValidateException(ActuatorConstant.STORE_NOT_EXIST);
    }
    AccountStore accountStore = chainBaseManager.getAccountStore();
    DynamicPropertiesStore dynamicStore = chainBaseManager.getDynamicPropertiesStore();
    if (!this.any.is(ExchangeCreateContract.class)) {
      throw new ContractValidateException(
          "contract type error,expected type [ExchangeCreateContract],real type[" + any
              .getClass() + "]");
    }
    final ExchangeCreateContract contract;
    try {
      contract = this.any.unpack(ExchangeCreateContract.class);
    } catch (InvalidProtocolBufferException e) {
      throw new ContractValidateException(e.getMessage());
    }

    byte[] ownerAddress = contract.getOwnerAddress().toByteArray();
    String readableOwnerAddress = StringUtil.createReadableString(ownerAddress);

    if (!DecodeUtil.addressValid(ownerAddress)) {
      throw new ContractValidateException("Invalid address");
    }

    if (!accountStore.has(ownerAddress)) {
      throw new ContractValidateException("account[" + readableOwnerAddress + NOT_EXIST_STR);
    }

    AccountCapsule accountCapsule = accountStore.get(ownerAddress);

    if (accountCapsule.getBalance().compareTo(BigInteger.valueOf(calcFee())) < 0) {
      throw new ContractValidateException("No enough balance for exchange create fee!");
    }

    byte[] firstTokenID = contract.getFirstTokenId().toByteArray();
    byte[] secondTokenID = contract.getSecondTokenId().toByteArray();
    BigInteger firstTokenBalance = BigIntegerUtil.newInstance(contract.getFirstTokenBalance());
    BigInteger secondTokenBalance = BigIntegerUtil.newInstance(contract.getSecondTokenBalance());

    if (dynamicStore.getAllowSameTokenName() == 1) {
      if (!Arrays.equals(firstTokenID, BIT_SYMBOL_BYTES) && !isNumber(firstTokenID)) {
        throw new ContractValidateException("first token id is not a valid number");
      }
      if (!Arrays.equals(secondTokenID, BIT_SYMBOL_BYTES) && !isNumber(secondTokenID)) {
        throw new ContractValidateException("second token id is not a valid number");
      }
    }

    if (Arrays.equals(firstTokenID, secondTokenID)) {
      throw new ContractValidateException("cannot exchange same tokens");
    }

    if (firstTokenBalance.compareTo(BigInteger.ZERO) <= 0 || secondTokenBalance.compareTo(BigInteger.ZERO) <= 0) {
      throw new ContractValidateException("token balance must greater than zero");
    }

    BigInteger balanceLimit = dynamicStore.getExchangeBalanceLimit();
    if (firstTokenBalance.compareTo(balanceLimit) > 0 || secondTokenBalance.compareTo(balanceLimit) > 0) {
      throw new ContractValidateException("token balance must less than " + balanceLimit);
    }

    if (Arrays.equals(firstTokenID, BIT_SYMBOL_BYTES)) {
      if (accountCapsule.getBalance().compareTo(firstTokenBalance.add(BigInteger.valueOf(calcFee()))) < 0) {
        throw new ContractValidateException("balance is not enough");
      }
    } else {
      if (!accountCapsule.assetBalanceEnoughV2(firstTokenID, firstTokenBalance, dynamicStore)) {
        throw new ContractValidateException("first token balance is not enough");
      }
    }

    if (Arrays.equals(secondTokenID, BIT_SYMBOL_BYTES)) {
      if (accountCapsule.getBalance().compareTo(secondTokenBalance.add(BigInteger.valueOf(calcFee()))) < 0) {
        throw new ContractValidateException("balance is not enough");
      }
    } else {
      if (!accountCapsule.assetBalanceEnoughV2(secondTokenID, secondTokenBalance, dynamicStore)) {
        throw new ContractValidateException("second token balance is not enough");
      }
    }

    return true;
  }


  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return any.unpack(ExchangeCreateContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return chainBaseManager.getDynamicPropertiesStore().getExchangeCreateFee();
  }

}
