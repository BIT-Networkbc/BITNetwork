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
import org.bit.common.utils.*;
import org.bit.core.capsule.AccountCapsule;
import org.bit.core.capsule.ExchangeCapsule;
import org.bit.core.capsule.TransactionResultCapsule;
import org.bit.core.exception.ContractExeException;
import org.bit.core.exception.ContractValidateException;
import org.bit.core.exception.ItemNotFoundException;
import org.bit.core.store.AccountStore;
import org.bit.core.store.AssetIssueStore;
import org.bit.core.store.DynamicPropertiesStore;
import org.bit.core.store.ExchangeStore;
import org.bit.core.store.ExchangeV2Store;
import org.bit.core.utils.TransactionUtil;
import org.bit.protos.Protocol.Transaction.Contract.ContractType;
import org.bit.protos.Protocol.Transaction.Result.code;
import org.bit.protos.contract.ExchangeContract.ExchangeTransactionContract;

@Slf4j(topic = "actuator")
public class ExchangeTransactionActuator extends AbstractActuator {

  public ExchangeTransactionActuator() {
    super(ContractType.ExchangeTransactionContract, ExchangeTransactionContract.class);
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
    ExchangeStore exchangeStore = chainBaseManager.getExchangeStore();
    ExchangeV2Store exchangeV2Store = chainBaseManager.getExchangeV2Store();
    AssetIssueStore assetIssueStore = chainBaseManager.getAssetIssueStore();
    try {
      final ExchangeTransactionContract exchangeTransactionContract = this.any
          .unpack(ExchangeTransactionContract.class);
      AccountCapsule accountCapsule = accountStore
          .get(exchangeTransactionContract.getOwnerAddress().toByteArray());

      ExchangeCapsule exchangeCapsule = Commons
          .getExchangeStoreFinal(dynamicStore, exchangeStore, exchangeV2Store).
              get(ByteArray.fromLong(exchangeTransactionContract.getExchangeId()));

      byte[] firstTokenID = exchangeCapsule.getFirstTokenId();
      byte[] secondTokenID = exchangeCapsule.getSecondTokenId();

      byte[] tokenID = exchangeTransactionContract.getTokenId().toByteArray();
      BigInteger tokenQuant = BigIntegerUtil.newInstance(exchangeTransactionContract.getQuant());

      byte[] anotherTokenID;
      BigInteger anotherTokenQuant = exchangeCapsule.transaction(tokenID, tokenQuant);

      if (Arrays.equals(tokenID, firstTokenID)) {
        anotherTokenID = secondTokenID;
      } else {
        anotherTokenID = firstTokenID;
      }

      BigInteger newBalance = accountCapsule.getBalance().subtract(BigInteger.valueOf(calcFee()));
      accountCapsule.setBalance(newBalance.toString());

      if (Arrays.equals(tokenID, BIT_SYMBOL_BYTES)) {
        accountCapsule.setBalance(newBalance.subtract(tokenQuant).toString());
      } else {
        accountCapsule.reduceAssetAmountV2(tokenID, tokenQuant, dynamicStore, assetIssueStore);
      }

      if (Arrays.equals(anotherTokenID, BIT_SYMBOL_BYTES)) {
        accountCapsule.setBalance(newBalance.add(anotherTokenQuant).toString());
      } else {
        accountCapsule
            .addAssetAmountV2(anotherTokenID, anotherTokenQuant, dynamicStore, assetIssueStore);
      }

      accountStore.put(accountCapsule.createDbKey(), accountCapsule);

      Commons.putExchangeCapsule(exchangeCapsule, dynamicStore, exchangeStore, exchangeV2Store,
          assetIssueStore);

      ret.setExchangeReceivedAmount(anotherTokenQuant.toString());
      ret.setStatus(fee, code.SUCESS);
    } catch (ItemNotFoundException | InvalidProtocolBufferException e) {
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
    ExchangeStore exchangeStore = chainBaseManager.getExchangeStore();
    ExchangeV2Store exchangeV2Store = chainBaseManager.getExchangeV2Store();
    if (!this.any.is(ExchangeTransactionContract.class)) {
      throw new ContractValidateException(
          "contract type error,expected type [ExchangeTransactionContract],real type[" + any
              .getClass() + "]");
    }
    final ExchangeTransactionContract contract;
    try {
      contract = this.any.unpack(ExchangeTransactionContract.class);
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
      throw new ContractValidateException("No enough balance for exchange transaction fee!");
    }

    ExchangeCapsule exchangeCapsule;
    try {
      exchangeCapsule = Commons.getExchangeStoreFinal(dynamicStore, exchangeStore, exchangeV2Store).
          get(ByteArray.fromLong(contract.getExchangeId()));
    } catch (ItemNotFoundException ex) {
      throw new ContractValidateException("Exchange[" + contract.getExchangeId()
          + ActuatorConstant.NOT_EXIST_STR);
    }

    byte[] firstTokenID = exchangeCapsule.getFirstTokenId();
    byte[] secondTokenID = exchangeCapsule.getSecondTokenId();
    BigInteger firstTokenBalance = exchangeCapsule.getFirstTokenBalance();
    BigInteger secondTokenBalance = exchangeCapsule.getSecondTokenBalance();

    byte[] tokenID = contract.getTokenId().toByteArray();
    BigInteger tokenQuant = BigIntegerUtil.newInstance(contract.getQuant());
    BigInteger tokenExpected = BigIntegerUtil.newInstance(contract.getExpected());

    if (dynamicStore.getAllowSameTokenName() == 1 &&
        !Arrays.equals(tokenID, BIT_SYMBOL_BYTES) &&
        !isNumber(tokenID)) {
      throw new ContractValidateException("token id is not a valid number");
    }
    if (!Arrays.equals(tokenID, firstTokenID) && !Arrays.equals(tokenID, secondTokenID)) {
      throw new ContractValidateException("token is not in exchange");
    }

    if (tokenQuant.compareTo(BigInteger.ZERO) <= 0) {
      throw new ContractValidateException("token quant must greater than zero");
    }

    if (tokenExpected.compareTo(BigInteger.ZERO) <= 0) {
      throw new ContractValidateException("token expected must greater than zero");
    }

    if (firstTokenBalance.equals(BigInteger.ZERO) || secondTokenBalance.equals(BigInteger.ZERO)) {
      throw new ContractValidateException("Token balance in exchange is equal with 0,"
          + "the exchange has been closed");
    }

    BigInteger balanceLimit = dynamicStore.getExchangeBalanceLimit();
    BigInteger tokenBalance = (Arrays.equals(tokenID, firstTokenID) ? firstTokenBalance
        : secondTokenBalance);
    tokenBalance = tokenBalance.add(tokenQuant);
    if (tokenBalance.compareTo(balanceLimit) > 0) {
      throw new ContractValidateException("token balance must less than " + balanceLimit);
    }

    if (Arrays.equals(tokenID, BIT_SYMBOL_BYTES)) {
      if (accountCapsule.getBalance().compareTo((tokenQuant.add(BigInteger.valueOf(calcFee())))) < 0) {
        throw new ContractValidateException("balance is not enough");
      }
    } else {
      if (!accountCapsule.assetBalanceEnoughV2(tokenID, tokenQuant, dynamicStore)) {
        throw new ContractValidateException("token balance is not enough");
      }
    }

    BigInteger anotherTokenQuant = exchangeCapsule.transaction(tokenID, tokenQuant);
    if (anotherTokenQuant.compareTo(tokenExpected) < 0) {
      throw new ContractValidateException("token required must greater than expected");
    }

    return true;
  }


  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return any.unpack(ExchangeTransactionContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return 0;
  }

}
