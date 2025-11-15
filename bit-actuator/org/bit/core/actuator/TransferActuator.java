package org.bit.core.actuator;

import static org.bit.core.config.Parameter.ChainConstant.TRANSFER_FEE;

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
import org.bit.core.capsule.ContractCapsule;
import org.bit.core.capsule.TransactionResultCapsule;
import org.bit.core.exception.BalanceInsufficientException;
import org.bit.core.exception.ContractExeException;
import org.bit.core.exception.ContractValidateException;
import org.bit.core.store.AccountStore;
import org.bit.core.store.DynamicPropertiesStore;
import org.bit.protos.Protocol.AccountType;
import org.bit.protos.Protocol.Transaction.Contract.ContractType;
import org.bit.protos.Protocol.Transaction.Result.code;
import org.bit.protos.contract.BalanceContract.TransferContract;

@Slf4j(topic = "actuator")
public class TransferActuator extends AbstractActuator {

  public TransferActuator() {
    super(ContractType.TransferContract, TransferContract.class);
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
    try {
      TransferContract transferContract = any.unpack(TransferContract.class);
      BigInteger amount = BigIntegerUtil.newInstance(transferContract.getAmount());
      byte[] toAddress = transferContract.getToAddress().toByteArray();
      byte[] ownerAddress = transferContract.getOwnerAddress().toByteArray();

      // if account with to_address does not exist, create it first.
      AccountCapsule toAccount = accountStore.get(toAddress);
      if (toAccount == null) {
        boolean withDefaultPermission =
            dynamicStore.getAllowMultiSign() == 1;
        toAccount = new AccountCapsule(ByteString.copyFrom(toAddress), AccountType.Normal,
            dynamicStore.getLatestBlockHeaderTimestamp(), withDefaultPermission, dynamicStore);
        accountStore.put(toAddress, toAccount);

        fee = fee + dynamicStore.getCreateNewAccountFeeInSystemContract();
      }

      Commons.adjustBalance(accountStore, ownerAddress, (amount.add(BigInteger.valueOf(fee))).negate());
      if (dynamicStore.supportBlackHoleOptimization()) {
        dynamicStore.burnBit(fee);
      } else {
        Commons.adjustBalance(accountStore, accountStore.getBlackhole(), BigInteger.valueOf(fee));
      }
      Commons.adjustBalance(accountStore, toAddress, amount);
      ret.setStatus(fee, code.SUCESS);
    } catch (BalanceInsufficientException | ArithmeticException | InvalidProtocolBufferException e) {
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
    if (!this.any.is(TransferContract.class)) {
      throw new ContractValidateException(
          "contract type error, expected type [TransferContract], real type [" + this.any
              .getClass() + "]");
    }
    long fee = calcFee();
    final TransferContract transferContract;
    try {
      transferContract = any.unpack(TransferContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }

    byte[] toAddress = transferContract.getToAddress().toByteArray();
    byte[] ownerAddress = transferContract.getOwnerAddress().toByteArray();
    BigInteger amount = BigIntegerUtil.newInstance(transferContract.getAmount());

    if (!DecodeUtil.addressValid(ownerAddress)) {
      throw new ContractValidateException("Invalid ownerAddress!");
    }
    if (!DecodeUtil.addressValid(toAddress)) {
      throw new ContractValidateException("Invalid toAddress!");
    }

    if (Arrays.equals(toAddress, ownerAddress)) {
      throw new ContractValidateException("Cannot transfer BIT to yourself.");
    }

    AccountCapsule ownerAccount = accountStore.get(ownerAddress);

    if (ownerAccount == null) {
      throw new ContractValidateException("Validate TransferContract error, no OwnerAccount.");
    }

    BigInteger balance = ownerAccount.getBalance();

    if (amount.compareTo(BigInteger.ZERO) <= 0) {
      throw new ContractValidateException("Amount must be greater than 0.");
    }

    try {
      AccountCapsule toAccount = accountStore.get(toAddress);
      if (toAccount == null) {
        fee = fee + dynamicStore.getCreateNewAccountFeeInSystemContract();
      }
      //after ForbidTransferToContract proposal, send bit to smartContract by actuator is not allowed.
      if (dynamicStore.getForbidTransferToContract() == 1
          && toAccount != null
          && toAccount.getType() == AccountType.Contract) {

        throw new ContractValidateException("Cannot transfer BIT to a smartContract.");

      }

      // after AllowTvmCompatibleEvm proposal, send bit to smartContract which version is one
      // by actuator is not allowed.
      if (dynamicStore.getAllowTvmCompatibleEvm() == 1
          && toAccount != null
          && toAccount.getType() == AccountType.Contract) {

        ContractCapsule contractCapsule = chainBaseManager.getContractStore().get(toAddress);
        if (contractCapsule == null) { //  this can not happen
          throw new ContractValidateException(
              "Account type is Contract, but it is not exist in contract store.");
        } else if (contractCapsule.getContractVersion() == 1) {
          throw new ContractValidateException(
              "Cannot transfer BIT to a smartContract which version is one. "
                  + "Instead please use TriggerSmartContract ");
        }
      }

      if (balance.compareTo(amount.add(BigInteger.valueOf(fee))) < 0) {
        logger.warn("Balance is not sufficient. Account: {}, balance: {}, amount: {}, fee: {}.",
            StringUtil.encode58Check(ownerAddress), balance, amount, fee);
        throw new ContractValidateException(
            "Validate TransferContract error, balance is not sufficient.");
      }

      if (toAccount != null) {
        toAccount.getBalance().add(amount);
      }
    } catch (ArithmeticException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }

    return true;
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return any.unpack(TransferContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return TRANSFER_FEE;
  }

}
