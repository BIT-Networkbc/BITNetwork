package org.bit.core.actuator;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.bit.common.utils.BigIntegerUtil;
import org.bit.common.utils.DecodeUtil;
import org.bit.common.utils.StringUtil;
import org.bit.core.capsule.AccountCapsule;
import org.bit.core.capsule.TransactionResultCapsule;
import org.bit.core.exception.ContractExeException;
import org.bit.core.exception.ContractValidateException;
import org.bit.core.store.AccountStore;
import org.bit.core.store.DynamicPropertiesStore;
import org.bit.protos.Protocol.Transaction.Contract.ContractType;
import org.bit.protos.Protocol.Transaction.Result.code;
import org.bit.protos.contract.BalanceContract.FreezeBalanceV2Contract;

import java.math.BigInteger;
import java.util.Objects;

import static org.bit.core.actuator.ActuatorConstant.NOT_EXIST_STR;
import static org.bit.core.config.Parameter.ChainConstant.BIT_PRECISION;
import static org.bit.protos.contract.Common.ResourceCode.BANDWIDTH;
import static org.bit.protos.contract.Common.ResourceCode.ENERGY;

@Slf4j(topic = "actuator")
public class FreezeBalanceV2Actuator extends AbstractActuator {

  public FreezeBalanceV2Actuator() {
    super(ContractType.FreezeBalanceV2Contract, FreezeBalanceV2Contract.class);
  }

  @Override
  public boolean execute(Object result) throws ContractExeException {
    TransactionResultCapsule ret = (TransactionResultCapsule) result;
    if (Objects.isNull(ret)) {
      throw new RuntimeException(ActuatorConstant.TX_RESULT_NULL);
    }

    long fee = calcFee();
    final FreezeBalanceV2Contract freezeBalanceV2Contract;
    AccountStore accountStore = chainBaseManager.getAccountStore();
    DynamicPropertiesStore dynamicStore = chainBaseManager.getDynamicPropertiesStore();
    try {
      freezeBalanceV2Contract = any.unpack(FreezeBalanceV2Contract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      ret.setStatus(fee, code.FAILED);
      throw new ContractExeException(e.getMessage());
    }
    AccountCapsule accountCapsule = accountStore.get(freezeBalanceV2Contract.getOwnerAddress().toByteArray());

    if (dynamicStore.supportAllowNewResourceModel()
        && accountCapsule.oldBitPowerIsNotInitialized()) {
      accountCapsule.initializeOldBitPower();
    }

    BigInteger frozenBalance = BigIntegerUtil.newInstance(freezeBalanceV2Contract.getFrozenBalance());
    BigInteger newBalance = accountCapsule.getBalance().subtract(frozenBalance);

    switch (freezeBalanceV2Contract.getResource()) {
      case BANDWIDTH:
        long oldNetWeight = accountCapsule.getFrozenV2BalanceWithDelegated(BANDWIDTH).divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        accountCapsule.addFrozenBalanceForBandwidthV2(frozenBalance);
        long newNetWeight = accountCapsule.getFrozenV2BalanceWithDelegated(BANDWIDTH).divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        dynamicStore.addTotalNetWeight(BigInteger.valueOf(newNetWeight - oldNetWeight));
        break;
      case ENERGY:
        long oldEnergyWeight = accountCapsule.getFrozenV2BalanceWithDelegated(ENERGY).divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        accountCapsule.addFrozenBalanceForEnergyV2(frozenBalance);
        long newEnergyWeight = accountCapsule.getFrozenV2BalanceWithDelegated(ENERGY).divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        dynamicStore.addTotalEnergyWeight(newEnergyWeight - oldEnergyWeight);
        break;
      case BIT_POWER:
        long oldTPWeight = accountCapsule.getBitPowerFrozenV2Balance().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        accountCapsule.addFrozenForBitPowerV2(frozenBalance);
        long newTPWeight = accountCapsule.getBitPowerFrozenV2Balance().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        dynamicStore.addTotalBitPowerWeight(newTPWeight - oldTPWeight);
        break;
      default:
        logger.debug("Resource Code Error.");
    }

    accountCapsule.setBalance(newBalance.toString());
    accountStore.put(accountCapsule.createDbKey(), accountCapsule);

    ret.setStatus(fee, code.SUCESS);

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
    if (!any.is(FreezeBalanceV2Contract.class)) {
      throw new ContractValidateException(
          "contract type error,expected type [FreezeBalanceV2Contract],real type[" + any
              .getClass() + "]");
    }

    if (!dynamicStore.supportUnfreezeDelay()) {
      throw new ContractValidateException("Not support FreezeV2 transaction,"
          + " need to be opened by the committee");
    }

    final FreezeBalanceV2Contract freezeBalanceV2Contract;
    try {
      freezeBalanceV2Contract = this.any.unpack(FreezeBalanceV2Contract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }
    byte[] ownerAddress = freezeBalanceV2Contract.getOwnerAddress().toByteArray();
    if (!DecodeUtil.addressValid(ownerAddress)) {
      throw new ContractValidateException("Invalid address");
    }

    AccountCapsule accountCapsule = accountStore.get(ownerAddress);
    if (accountCapsule == null) {
      String readableOwnerAddress = StringUtil.createReadableString(ownerAddress);
      throw new ContractValidateException(
          ActuatorConstant.ACCOUNT_EXCEPTION_STR + readableOwnerAddress + NOT_EXIST_STR);
    }

    BigInteger frozenBalance = BigIntegerUtil.newInstance(freezeBalanceV2Contract.getFrozenBalance());
    if (frozenBalance.compareTo(BigInteger.ZERO) <= 0) {
      throw new ContractValidateException("frozenBalance must be positive");
    }
    if (frozenBalance.compareTo(BigInteger.valueOf(BIT_PRECISION)) < 0) {
      throw new ContractValidateException("frozenBalance must be greater than or equal to 1 BIT");
    }

    if (frozenBalance.compareTo(accountCapsule.getBalance()) > 0) {
      throw new ContractValidateException("frozenBalance must be less than or equal to accountBalance");
    }

    switch (freezeBalanceV2Contract.getResource()) {
      case BANDWIDTH:
      case ENERGY:
        break;
      case BIT_POWER:
        if (!dynamicStore.supportAllowNewResourceModel()) {
          throw new ContractValidateException(
              "ResourceCode error, valid ResourceCode[BANDWIDTH、ENERGY]");
        }
        break;
      default:
        if (dynamicStore.supportAllowNewResourceModel()) {
          throw new ContractValidateException(
              "ResourceCode error, valid ResourceCode[BANDWIDTH、ENERGY、BIT_POWER]");
        } else {
          throw new ContractValidateException(
              "ResourceCode error, valid ResourceCode[BANDWIDTH、ENERGY]");
        }
    }

    return true;
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return any.unpack(FreezeBalanceV2Contract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return 0;
  }

}