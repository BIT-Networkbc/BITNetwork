package org.bit.core.actuator;

import static org.bit.core.actuator.ActuatorConstant.ACCOUNT_EXCEPTION_STR;
import static org.bit.core.actuator.ActuatorConstant.NOT_EXIST_STR;
import static org.bit.core.config.Parameter.ChainConstant.BIT_PRECISION;
import static org.bit.protos.contract.Common.ResourceCode.BANDWIDTH;
import static org.bit.protos.contract.Common.ResourceCode.ENERGY;
import static org.bit.protos.contract.Common.ResourceCode.BIT_POWER;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.bit.common.utils.BigIntegerUtil;
import org.bit.common.utils.DecodeUtil;
import org.bit.common.utils.StringUtil;
import org.bit.core.capsule.AccountCapsule;
import org.bit.core.capsule.TransactionResultCapsule;
import org.bit.core.exception.ContractExeException;
import org.bit.core.exception.ContractValidateException;
import org.bit.core.store.AccountStore;
import org.bit.core.store.DynamicPropertiesStore;
import org.bit.protos.Protocol.Account.UnFreezeV2;
import org.bit.protos.Protocol.Transaction.Contract.ContractType;
import org.bit.protos.Protocol.Transaction.Result.code;
import org.bit.protos.contract.BalanceContract.CancelAllUnfreezeV2Contract;

@Slf4j(topic = "actuator")
public class CancelAllUnfreezeV2Actuator extends AbstractActuator {

  public CancelAllUnfreezeV2Actuator() {
    super(ContractType.CancelAllUnfreezeV2Contract, CancelAllUnfreezeV2Contract.class);
  }

  @Override
  public boolean execute(Object result) throws ContractExeException {
    TransactionResultCapsule ret = (TransactionResultCapsule) result;
    if (Objects.isNull(ret)) {
      throw new RuntimeException(ActuatorConstant.TX_RESULT_NULL);
    }
    long fee = calcFee();
    AccountStore accountStore = chainBaseManager.getAccountStore();
    DynamicPropertiesStore dynamicStore = chainBaseManager.getDynamicPropertiesStore();
    byte[] ownerAddress;
    try {
      ownerAddress = getOwnerAddress().toByteArray();
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      ret.setStatus(fee, code.FAILED);
      throw new ContractExeException(e.getMessage());
    }
    AccountCapsule ownerCapsule = accountStore.get(ownerAddress);
    List<UnFreezeV2> unfrozenV2List = ownerCapsule.getUnfrozenV2List();
    long now = dynamicStore.getLatestBlockHeaderTimestamp();
    AtomicReference<BigInteger> atomicWithdrawExpireBalance = new AtomicReference<BigInteger>(BigInteger.ZERO);
    /* The triple object is defined by resource type, with left representing the pair object
    corresponding to bandwidth, middle representing the pair object corresponding to energy, and
    right representing the pair object corresponding to bit power. The pair object for each
    resource type, left represents resource weight, and right represents the number of unfreeze
    resources for that resource type. */
    Triple<Pair<AtomicReference<BigInteger>, AtomicReference<BigInteger>>, Pair<AtomicReference<BigInteger>, AtomicReference<BigInteger>>, Pair<AtomicReference<BigInteger>, AtomicReference<BigInteger>>>
        triple = Triple.of(
        Pair.of(new AtomicReference<BigInteger>(BigInteger.ZERO), new AtomicReference<BigInteger>(BigInteger.ZERO)),
        Pair.of(new AtomicReference<BigInteger>(BigInteger.ZERO), new AtomicReference<BigInteger>(BigInteger.ZERO)),
        Pair.of(new AtomicReference<BigInteger>(BigInteger.ZERO), new AtomicReference<BigInteger>(BigInteger.ZERO)));
    for (UnFreezeV2 unFreezeV2 : unfrozenV2List) {
      updateAndCalculate(triple, ownerCapsule, now, atomicWithdrawExpireBalance, unFreezeV2);
    }
    ownerCapsule.clearUnfrozenV2();
    addTotalResourceWeight(dynamicStore, triple);

    BigInteger withdrawExpireBalance = atomicWithdrawExpireBalance.get();
    if (withdrawExpireBalance.compareTo(BigInteger.ZERO) > 0) {
      ownerCapsule.setBalance(ownerCapsule.getBalance().add(withdrawExpireBalance).toString());
    }

    accountStore.put(ownerCapsule.createDbKey(), ownerCapsule);
    ret.setWithdrawExpireAmount(withdrawExpireBalance.toString());
    Map<String, BigInteger> cancelUnfreezeV2AmountMap = new HashMap<>();
    cancelUnfreezeV2AmountMap.put(BANDWIDTH.name(), triple.getLeft().getRight().get());
    cancelUnfreezeV2AmountMap.put(ENERGY.name(), triple.getMiddle().getRight().get());
    cancelUnfreezeV2AmountMap.put(BIT_POWER.name(), triple.getRight().getRight().get());
    ret.putAllCancelUnfreezeV2AmountMap(cancelUnfreezeV2AmountMap.entrySet().stream()
            .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().toString()
            )));
    ret.setStatus(fee, code.SUCESS);
    return true;
  }

  private void addTotalResourceWeight(DynamicPropertiesStore dynamicStore,
      Triple<Pair<AtomicReference<BigInteger>, AtomicReference<BigInteger>>,
          Pair<AtomicReference<BigInteger>, AtomicReference<BigInteger>>,
          Pair<AtomicReference<BigInteger>, AtomicReference<BigInteger>>> triple) {
    dynamicStore.addTotalNetWeight(triple.getLeft().getLeft().get());
    dynamicStore.addTotalEnergyWeight(triple.getMiddle().getLeft().get().longValue());
    dynamicStore.addTotalBitPowerWeight(triple.getRight().getLeft().get().longValue());
  }

  private void updateAndCalculate(Triple<Pair<AtomicReference<BigInteger>, AtomicReference<BigInteger>>, Pair<AtomicReference<BigInteger>, AtomicReference<BigInteger>>,
      Pair<AtomicReference<BigInteger>, AtomicReference<BigInteger>>> triple,
      AccountCapsule ownerCapsule, long now, AtomicReference<BigInteger> atomicLong, UnFreezeV2 unFreezeV2) {
    if (unFreezeV2.getUnfreezeExpireTime() > now) {
      updateFrozenInfoAndTotalResourceWeight(ownerCapsule, unFreezeV2, triple);
    } else {
      atomicLong.accumulateAndGet(BigIntegerUtil.newInstance(unFreezeV2.getUnfreezeAmount()), (u, v) -> u = u.add(v));
    }
  }

  @Override
  public boolean validate() throws ContractValidateException {
    if (Objects.isNull(this.any)) {
      throw new ContractValidateException(ActuatorConstant.CONTRACT_NOT_EXIST);
    }

    if (Objects.isNull(chainBaseManager)) {
      throw new ContractValidateException(ActuatorConstant.STORE_NOT_EXIST);
    }

    AccountStore accountStore = chainBaseManager.getAccountStore();
    DynamicPropertiesStore dynamicStore = chainBaseManager.getDynamicPropertiesStore();

    if (!this.any.is(CancelAllUnfreezeV2Contract.class)) {
      throw new ContractValidateException("contract type error, expected type " +
          "[CancelAllUnfreezeV2Contract], real type[" + any.getClass() + "]");
    }

    if (!dynamicStore.supportAllowCancelAllUnfreezeV2()) {
      throw new ContractValidateException("Not support CancelAllUnfreezeV2 transaction,"
          + " need to be opened by the committee");
    }

    byte[] ownerAddress;
    try {
      ownerAddress = getOwnerAddress().toByteArray();
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }

    if (!DecodeUtil.addressValid(ownerAddress)) {
      throw new ContractValidateException("Invalid address");
    }
    AccountCapsule accountCapsule = accountStore.get(ownerAddress);
    String readableOwnerAddress = StringUtil.createReadableString(ownerAddress);
    if (Objects.isNull(accountCapsule)) {
      throw new ContractValidateException(ACCOUNT_EXCEPTION_STR
          + readableOwnerAddress + NOT_EXIST_STR);
    }

    List<UnFreezeV2> unfrozenV2List = accountCapsule.getUnfrozenV2List();
    if (unfrozenV2List.isEmpty()) {
      throw new ContractValidateException("No unfreezeV2 list to cancel");
    }

    return true;
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return getCancelAllUnfreezeV2Contract().getOwnerAddress();
  }

  private CancelAllUnfreezeV2Contract getCancelAllUnfreezeV2Contract()
      throws InvalidProtocolBufferException {
    return any.unpack(CancelAllUnfreezeV2Contract.class);
  }

  @Override
  public long calcFee() {
    return 0;
  }

  public void updateFrozenInfoAndTotalResourceWeight(
      AccountCapsule accountCapsule, UnFreezeV2 unFreezeV2,
      Triple<Pair<AtomicReference<BigInteger>, AtomicReference<BigInteger>>, Pair<AtomicReference<BigInteger>, AtomicReference<BigInteger>>,
          Pair<AtomicReference<BigInteger>, AtomicReference<BigInteger>>> triple) {
    switch (unFreezeV2.getType()) {
      case BANDWIDTH:
        long oldNetWeight = accountCapsule.getFrozenV2BalanceWithDelegated(BANDWIDTH).divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        accountCapsule.addFrozenBalanceForBandwidthV2(BigIntegerUtil.newInstance(unFreezeV2.getUnfreezeAmount()));
        long newNetWeight = accountCapsule.getFrozenV2BalanceWithDelegated(BANDWIDTH).divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        triple.getLeft().getLeft().accumulateAndGet(BigInteger.valueOf(newNetWeight - oldNetWeight), (u, v) -> u = u.add(v));
        triple.getLeft().getRight().accumulateAndGet(BigIntegerUtil.newInstance(unFreezeV2.getUnfreezeAmount()), (u, v) -> u = u.add(v));
        break;
      case ENERGY:
        long oldEnergyWeight = accountCapsule.getFrozenV2BalanceWithDelegated(ENERGY).divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        accountCapsule.addFrozenBalanceForEnergyV2(BigIntegerUtil.newInstance(unFreezeV2.getUnfreezeAmount()));
        long newEnergyWeight = accountCapsule.getFrozenV2BalanceWithDelegated(ENERGY).divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        triple.getMiddle().getLeft().accumulateAndGet(BigInteger.valueOf(newEnergyWeight - oldEnergyWeight), (u, v) -> u = u.add(v));
        triple.getMiddle().getRight().accumulateAndGet(BigIntegerUtil.newInstance(unFreezeV2.getUnfreezeAmount()), (u, v) -> u = u.add(v));
        break;
      case BIT_POWER:
        long oldTPWeight = accountCapsule.getBitPowerFrozenV2Balance().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        accountCapsule.addFrozenForBitPowerV2(BigIntegerUtil.newInstance(unFreezeV2.getUnfreezeAmount()));
        long newTPWeight = accountCapsule.getBitPowerFrozenV2Balance().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        triple.getRight().getLeft().accumulateAndGet(BigInteger.valueOf(newTPWeight - oldTPWeight), (u, v) -> u = u.add(v));
        triple.getRight().getRight().accumulateAndGet(BigIntegerUtil.newInstance(unFreezeV2.getUnfreezeAmount()), (u, v) -> u = u.add(v));
        break;
      default:
        break;
    }
  }
}
