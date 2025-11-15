package org.bit.core.actuator;

import static org.bit.core.actuator.ActuatorConstant.ACCOUNT_EXCEPTION_STR;
import static org.bit.core.actuator.ActuatorConstant.NOT_EXIST_STR;

import com.google.common.math.LongMath;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
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
import org.bit.protos.Protocol.Account.UnFreezeV2;
import org.bit.protos.Protocol.Transaction.Contract.ContractType;
import org.bit.protos.Protocol.Transaction.Result.code;
import org.bit.protos.contract.BalanceContract.WithdrawExpireUnfreezeContract;


@Slf4j(topic = "actuator")
public class WithdrawExpireUnfreezeActuator extends AbstractActuator {

  public WithdrawExpireUnfreezeActuator() {
    super(ContractType.WithdrawExpireUnfreezeContract, WithdrawExpireUnfreezeContract.class);
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
    final WithdrawExpireUnfreezeContract withdrawExpireUnfreezeContract;
    try {
      withdrawExpireUnfreezeContract = any.unpack(WithdrawExpireUnfreezeContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      ret.setStatus(fee, code.FAILED);
      throw new ContractExeException(e.getMessage());
    }
    AccountCapsule accountCapsule = accountStore.get(
        withdrawExpireUnfreezeContract.getOwnerAddress().toByteArray());
    long now = dynamicStore.getLatestBlockHeaderTimestamp();
    List<UnFreezeV2> unfrozenV2List = accountCapsule.getInstance().getUnfrozenV2List();
    BigInteger totalWithdrawUnfreeze = getTotalWithdrawUnfreeze(unfrozenV2List, now);
    accountCapsule.setInstance(accountCapsule.getInstance().toBuilder()
        .setBalance(accountCapsule.getBalance().add(totalWithdrawUnfreeze).toString())
        .build());
    List<UnFreezeV2> newUnFreezeList = getRemainWithdrawList(unfrozenV2List, now);
    accountCapsule.clearUnfrozenV2();
    accountCapsule.addAllUnfrozenV2(newUnFreezeList);
    accountStore.put(accountCapsule.createDbKey(), accountCapsule);
    ret.setWithdrawExpireAmount(totalWithdrawUnfreeze.toString());
    ret.setStatus(fee, code.SUCESS);
    return true;
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
    if (!this.any.is(WithdrawExpireUnfreezeContract.class)) {
      throw new ContractValidateException(
          "contract type error, expected type [WithdrawExpireUnfreezeContract], real type[" + any
              .getClass() + "]");
    }

    if (!dynamicStore.supportUnfreezeDelay()) {
      throw new ContractValidateException("Not support WithdrawExpireUnfreeze transaction,"
          + " need to be opened by the committee");
    }

    final WithdrawExpireUnfreezeContract withdrawExpireUnfreezeContract;
    try {
      withdrawExpireUnfreezeContract = this.any.unpack(WithdrawExpireUnfreezeContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }
    byte[] ownerAddress = withdrawExpireUnfreezeContract.getOwnerAddress().toByteArray();
    if (!DecodeUtil.addressValid(ownerAddress)) {
      throw new ContractValidateException("Invalid address");
    }
    AccountCapsule accountCapsule = accountStore.get(ownerAddress);
    String readableOwnerAddress = StringUtil.createReadableString(ownerAddress);
    if (Objects.isNull(accountCapsule)) {
      throw new ContractValidateException(ACCOUNT_EXCEPTION_STR
          + readableOwnerAddress + NOT_EXIST_STR);
    }

    long now = dynamicStore.getLatestBlockHeaderTimestamp();
    List<UnFreezeV2> unfrozenV2List = accountCapsule.getInstance().getUnfrozenV2List();
    BigInteger totalWithdrawUnfreeze = getTotalWithdrawUnfreeze(unfrozenV2List, now);
    if (totalWithdrawUnfreeze.compareTo(BigInteger.ZERO) <= 0) {
      throw new ContractValidateException("no unFreeze balance to withdraw ");
    }
//    try {
//      LongMath.checkedAdd(accountCapsule.getBalance(), totalWithdrawUnfreeze);
//    } catch (ArithmeticException e) {
//      logger.debug(e.getMessage(), e);
//      throw new ContractValidateException(e.getMessage());
//    }
    return true;
  }

  private BigInteger getTotalWithdrawUnfreeze(List<UnFreezeV2> unfrozenV2List, long now) {
    return getTotalWithdrawList(unfrozenV2List, now).stream()
        .map(o -> BigIntegerUtil.newInstance(o.getUnfreezeAmount())).reduce(BigInteger.ZERO,BigInteger::add);
  }

  private List<UnFreezeV2> getTotalWithdrawList(List<UnFreezeV2> unfrozenV2List, long now) {
    return unfrozenV2List.stream().filter(unfrozenV2 -> unfrozenV2.getUnfreezeExpireTime() <= now)
        .collect(Collectors.toList());
  }

  private List<UnFreezeV2> getRemainWithdrawList(List<UnFreezeV2> unfrozenV2List, long now) {
    return unfrozenV2List.stream()
        .filter(unfrozenV2 -> unfrozenV2.getUnfreezeExpireTime() > now)
        .collect(Collectors.toList());
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return any.unpack(WithdrawExpireUnfreezeContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return 0;
  }

}
