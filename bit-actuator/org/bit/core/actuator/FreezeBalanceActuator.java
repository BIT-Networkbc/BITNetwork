package org.bit.core.actuator;

import static org.bit.core.actuator.ActuatorConstant.NOT_EXIST_STR;
import static org.bit.core.config.Parameter.ChainConstant.FROZEN_PERIOD;
import static org.bit.core.config.Parameter.ChainConstant.BIT_PRECISION;
import static org.bit.protos.contract.Common.ResourceCode;
import static org.bit.protos.contract.Common.ResourceCode.BANDWIDTH;
import static org.bit.protos.contract.Common.ResourceCode.ENERGY;
import static org.bit.protos.contract.Common.ResourceCode.BIT_POWER;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.bit.common.parameter.CommonParameter;
import org.bit.common.utils.BigIntegerUtil;
import org.bit.common.utils.DecodeUtil;
import org.bit.common.utils.StringUtil;
import org.bit.core.capsule.AccountCapsule;
import org.bit.core.capsule.DelegatedResourceAccountIndexCapsule;
import org.bit.core.capsule.DelegatedResourceCapsule;
import org.bit.core.capsule.TransactionResultCapsule;
import org.bit.core.exception.ContractExeException;
import org.bit.core.exception.ContractValidateException;
import org.bit.core.store.AccountStore;
import org.bit.core.store.DelegatedResourceAccountIndexStore;
import org.bit.core.store.DelegatedResourceStore;
import org.bit.core.store.DynamicPropertiesStore;
import org.bit.protos.Protocol.AccountType;
import org.bit.protos.Protocol.Transaction.Contract.ContractType;
import org.bit.protos.Protocol.Transaction.Result.code;
import org.bit.protos.contract.BalanceContract.FreezeBalanceContract;

@Slf4j(topic = "actuator")
public class FreezeBalanceActuator extends AbstractActuator {

  public FreezeBalanceActuator() {
    super(ContractType.FreezeBalanceContract, FreezeBalanceContract.class);
  }

  @Override
  public boolean execute(Object result) throws ContractExeException {
    TransactionResultCapsule ret = (TransactionResultCapsule) result;
    if (Objects.isNull(ret)) {
      throw new RuntimeException(ActuatorConstant.TX_RESULT_NULL);
    }

    long fee = calcFee();
    final FreezeBalanceContract freezeBalanceContract;
    AccountStore accountStore = chainBaseManager.getAccountStore();
    DynamicPropertiesStore dynamicStore = chainBaseManager.getDynamicPropertiesStore();
    try {
      freezeBalanceContract = any.unpack(FreezeBalanceContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      ret.setStatus(fee, code.FAILED);
      throw new ContractExeException(e.getMessage());
    }
    AccountCapsule accountCapsule = accountStore
        .get(freezeBalanceContract.getOwnerAddress().toByteArray());

    if (dynamicStore.supportAllowNewResourceModel()
        && accountCapsule.oldBitPowerIsNotInitialized()) {
      accountCapsule.initializeOldBitPower();
    }

    long now = dynamicStore.getLatestBlockHeaderTimestamp();
    long duration = freezeBalanceContract.getFrozenDuration() * FROZEN_PERIOD;

    BigInteger newBalance = accountCapsule.getBalance().subtract(BigIntegerUtil.newInstance(freezeBalanceContract.getFrozenBalance()));

    BigInteger frozenBalance = BigIntegerUtil.newInstance(freezeBalanceContract.getFrozenBalance());
    long expireTime = now + duration;
    byte[] ownerAddress = freezeBalanceContract.getOwnerAddress().toByteArray();
    byte[] receiverAddress = freezeBalanceContract.getReceiverAddress().toByteArray();

    long increment;
    switch (freezeBalanceContract.getResource()) {
      case BANDWIDTH:
        if (!ArrayUtils.isEmpty(receiverAddress)
            && dynamicStore.supportDR()) {
          increment = delegateResource(ownerAddress, receiverAddress, true,
                  frozenBalance, expireTime);
          accountCapsule.addDelegatedFrozenBalanceForBandwidth(frozenBalance);
        } else {
          long oldNetWeight = accountCapsule.getFrozenBalance().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
          BigInteger newFrozenBalanceForBandwidth =
              frozenBalance.add(accountCapsule.getFrozenBalance());
          accountCapsule.setFrozenForBandwidth(newFrozenBalanceForBandwidth.toString(), expireTime);
          long newNetWeight = accountCapsule.getFrozenBalance().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
          increment = newNetWeight - oldNetWeight;
        }
        addTotalWeight(BANDWIDTH, dynamicStore, frozenBalance, increment);
        break;
      case ENERGY:
        if (!ArrayUtils.isEmpty(receiverAddress)
            && dynamicStore.supportDR()) {
          increment = delegateResource(ownerAddress, receiverAddress, false,
                  frozenBalance, expireTime);
          accountCapsule.addDelegatedFrozenBalanceForEnergy(frozenBalance);
        } else {
          long oldEnergyWeight = accountCapsule.getEnergyFrozenBalance().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
          BigInteger newFrozenBalanceForEnergy =
              frozenBalance.add(accountCapsule.getEnergyFrozenBalance());
          accountCapsule.setFrozenForEnergy(newFrozenBalanceForEnergy.toString(), expireTime);
          long newEnergyWeight = accountCapsule.getEnergyFrozenBalance().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
          increment = newEnergyWeight - oldEnergyWeight;
        }
        addTotalWeight(ENERGY, dynamicStore, frozenBalance, increment);
        break;
      case BIT_POWER:
        long oldTPWeight = accountCapsule.getBitPowerFrozenBalance().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        BigInteger newFrozenBalanceForBitPower =
            frozenBalance.add(accountCapsule.getBitPowerFrozenBalance());
        accountCapsule.setFrozenForBitPower(newFrozenBalanceForBitPower.toString(), expireTime);
        long newTPWeight = accountCapsule.getBitPowerFrozenBalance().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        increment = newTPWeight - oldTPWeight;
        addTotalWeight(BIT_POWER, dynamicStore, frozenBalance, increment);
        break;
      default:
        logger.debug("Resource Code Error.");
    }

    accountCapsule.setBalance(newBalance.toString());
    accountStore.put(accountCapsule.createDbKey(), accountCapsule);

    ret.setStatus(fee, code.SUCESS);

    return true;
  }

  private void addTotalWeight(ResourceCode resourceCode, DynamicPropertiesStore dynamicStore,
                              BigInteger frozenBalance, long increment) {
    long weight = dynamicStore.allowNewReward() ? increment : frozenBalance.divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
    switch (resourceCode) {
      case BANDWIDTH:
        dynamicStore.addTotalNetWeight(BigInteger.valueOf(weight));
        break;
      case ENERGY:
        dynamicStore.addTotalEnergyWeight(weight);
        break;
      case BIT_POWER:
        dynamicStore.addTotalBitPowerWeight(weight);
        break;
      default:
        logger.debug("Resource Code Error.");
    }
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
    if (!any.is(FreezeBalanceContract.class)) {
      throw new ContractValidateException(
          "contract type error,expected type [FreezeBalanceContract],real type[" + any
              .getClass() + "]");
    }

    final FreezeBalanceContract freezeBalanceContract;
    try {
      freezeBalanceContract = this.any.unpack(FreezeBalanceContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }
    byte[] ownerAddress = freezeBalanceContract.getOwnerAddress().toByteArray();
    if (!DecodeUtil.addressValid(ownerAddress)) {
      throw new ContractValidateException("Invalid address");
    }

    AccountCapsule accountCapsule = accountStore.get(ownerAddress);
    if (accountCapsule == null) {
      String readableOwnerAddress = StringUtil.createReadableString(ownerAddress);
      throw new ContractValidateException(
          ActuatorConstant.ACCOUNT_EXCEPTION_STR + readableOwnerAddress + NOT_EXIST_STR);
    }

    BigInteger frozenBalance = BigIntegerUtil.newInstance(freezeBalanceContract.getFrozenBalance());
    if (frozenBalance.compareTo(BigInteger.ZERO) <= 0) {
      throw new ContractValidateException("frozenBalance must be positive");
    }
    if (frozenBalance.compareTo(BigInteger.valueOf(BIT_PRECISION)) < 0) {
      throw new ContractValidateException("frozenBalance must be greater than or equal to 1 BIT");
    }

    int frozenCount = accountCapsule.getFrozenCount();
    if (!(frozenCount == 0 || frozenCount == 1)) {
      throw new ContractValidateException("frozenCount must be 0 or 1");
    }
    if (frozenBalance.compareTo(accountCapsule.getBalance()) > 0) {
      throw new ContractValidateException("frozenBalance must be less than or equal to accountBalance");
    }

    long frozenDuration = freezeBalanceContract.getFrozenDuration();
    long minFrozenTime = dynamicStore.getMinFrozenTime();
    long maxFrozenTime = dynamicStore.getMaxFrozenTime();

    boolean needCheckFrozeTime = CommonParameter.getInstance()
        .getCheckFrozenTime() == 1;//for test
    if (needCheckFrozeTime && !(frozenDuration >= minFrozenTime
        && frozenDuration <= maxFrozenTime)) {
      throw new ContractValidateException(
          "frozenDuration must be less than " + maxFrozenTime + " days "
              + "and more than " + minFrozenTime + " days");
    }

    switch (freezeBalanceContract.getResource()) {
      case BANDWIDTH:
      case ENERGY:
        break;
      case BIT_POWER:
        if (dynamicStore.supportAllowNewResourceModel()) {
          byte[] receiverAddress = freezeBalanceContract.getReceiverAddress().toByteArray();
          if (!ArrayUtils.isEmpty(receiverAddress)) {
            throw new ContractValidateException(
                "BIT_POWER is not allowed to delegate to other accounts.");
          }
        } else {
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

    //todo：need version control and config for delegating resource
    byte[] receiverAddress = freezeBalanceContract.getReceiverAddress().toByteArray();
    //If the receiver is included in the contract, the receiver will receive the resource.
    if (!ArrayUtils.isEmpty(receiverAddress) && dynamicStore.supportDR()) {
      if (Arrays.equals(receiverAddress, ownerAddress)) {
        throw new ContractValidateException("receiverAddress must not be the same as ownerAddress");
      }

      if (!DecodeUtil.addressValid(receiverAddress)) {
        throw new ContractValidateException("Invalid receiverAddress");
      }

      AccountCapsule receiverCapsule = accountStore.get(receiverAddress);
      if (receiverCapsule == null) {
        String readableOwnerAddress = StringUtil.createReadableString(receiverAddress);
        throw new ContractValidateException(
            ActuatorConstant.ACCOUNT_EXCEPTION_STR
                + readableOwnerAddress + NOT_EXIST_STR);
      }

      if (dynamicStore.getAllowTvmConstantinople() == 1
          && receiverCapsule.getType() == AccountType.Contract) {
        throw new ContractValidateException(
            "Do not allow delegate resources to contract addresses");

      }

    }

    if (dynamicStore.supportUnfreezeDelay()) {
      throw new ContractValidateException(
              "freeze v2 is open, old freeze is closed");
    }

    return true;
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return any.unpack(FreezeBalanceContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return 0;
  }

  private long delegateResource(byte[] ownerAddress, byte[] receiverAddress, boolean isBandwidth,
      BigInteger balance, long expireTime) {
    AccountStore accountStore = chainBaseManager.getAccountStore();
    DynamicPropertiesStore dynamicPropertiesStore = chainBaseManager.getDynamicPropertiesStore();
    DelegatedResourceStore delegatedResourceStore = chainBaseManager.getDelegatedResourceStore();
    DelegatedResourceAccountIndexStore delegatedResourceAccountIndexStore = chainBaseManager
        .getDelegatedResourceAccountIndexStore();
    byte[] key = DelegatedResourceCapsule.createDbKey(ownerAddress, receiverAddress);
    //modify DelegatedResourceStore
    DelegatedResourceCapsule delegatedResourceCapsule = delegatedResourceStore
        .get(key);
    if (delegatedResourceCapsule != null) {
      if (isBandwidth) {
        delegatedResourceCapsule.addFrozenBalanceForBandwidth(balance, expireTime);
      } else {
        delegatedResourceCapsule.addFrozenBalanceForEnergy(balance, expireTime);
      }
    } else {
      delegatedResourceCapsule = new DelegatedResourceCapsule(
          ByteString.copyFrom(ownerAddress),
          ByteString.copyFrom(receiverAddress));
      if (isBandwidth) {
        delegatedResourceCapsule.setFrozenBalanceForBandwidth(balance.toString(), expireTime);
      } else {
        delegatedResourceCapsule.setFrozenBalanceForEnergy(balance.toString(), expireTime);
      }

    }
    delegatedResourceStore.put(key, delegatedResourceCapsule);

    //modify DelegatedResourceAccountIndexStore
    if (!dynamicPropertiesStore.supportAllowDelegateOptimization()) {

      DelegatedResourceAccountIndexCapsule ownerIndexCapsule =
          delegatedResourceAccountIndexStore.get(ownerAddress);
      if (ownerIndexCapsule == null) {
        ownerIndexCapsule = new DelegatedResourceAccountIndexCapsule(
            ByteString.copyFrom(ownerAddress));
      }
      List<ByteString> toAccountsList = ownerIndexCapsule.getToAccountsList();
      if (!toAccountsList.contains(ByteString.copyFrom(receiverAddress))) {
        ownerIndexCapsule.addToAccount(ByteString.copyFrom(receiverAddress));
      }
      delegatedResourceAccountIndexStore.put(ownerAddress, ownerIndexCapsule);

      DelegatedResourceAccountIndexCapsule receiverIndexCapsule
          = delegatedResourceAccountIndexStore.get(receiverAddress);
      if (receiverIndexCapsule == null) {
        receiverIndexCapsule = new DelegatedResourceAccountIndexCapsule(
            ByteString.copyFrom(receiverAddress));
      }
      List<ByteString> fromAccountsList = receiverIndexCapsule
          .getFromAccountsList();
      if (!fromAccountsList.contains(ByteString.copyFrom(ownerAddress))) {
        receiverIndexCapsule.addFromAccount(ByteString.copyFrom(ownerAddress));
      }
      delegatedResourceAccountIndexStore.put(receiverAddress, receiverIndexCapsule);

    } else {
      // modify DelegatedResourceAccountIndexStore new
      delegatedResourceAccountIndexStore.convert(ownerAddress);
      delegatedResourceAccountIndexStore.convert(receiverAddress);
      delegatedResourceAccountIndexStore.delegate(ownerAddress, receiverAddress,
          dynamicPropertiesStore.getLatestBlockHeaderTimestamp());
    }

    //modify AccountStore
    AccountCapsule receiverCapsule = accountStore.get(receiverAddress);
    long oldWeight;
    long newWeight;
    if (isBandwidth) {
      oldWeight = receiverCapsule.getAcquiredDelegatedFrozenBalanceForBandwidth().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
      receiverCapsule.addAcquiredDelegatedFrozenBalanceForBandwidth(balance);
      newWeight = receiverCapsule.getAcquiredDelegatedFrozenBalanceForBandwidth().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
    } else {
      oldWeight = receiverCapsule.getAcquiredDelegatedFrozenBalanceForEnergy().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
      receiverCapsule.addAcquiredDelegatedFrozenBalanceForEnergy(balance);
      newWeight = receiverCapsule.getAcquiredDelegatedFrozenBalanceForEnergy().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
    }
    accountStore.put(receiverCapsule.createDbKey(), receiverCapsule);
    return newWeight - oldWeight;
  }

}
