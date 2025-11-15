package org.bit.core.actuator;

import static org.bit.core.actuator.ActuatorConstant.ACCOUNT_EXCEPTION_STR;
import static org.bit.core.config.Parameter.ChainConstant.BIT_PRECISION;
import static org.bit.protos.contract.Common.ResourceCode.BANDWIDTH;
import static org.bit.protos.contract.Common.ResourceCode.ENERGY;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.bit.common.utils.BigIntegerUtil;
import org.bit.common.utils.DecodeUtil;
import org.bit.common.utils.StringUtil;
import org.bit.core.capsule.AccountCapsule;
import org.bit.core.capsule.DelegatedResourceCapsule;
import org.bit.core.capsule.TransactionResultCapsule;
import org.bit.core.db.BandwidthProcessor;
import org.bit.core.db.EnergyProcessor;
import org.bit.core.exception.ContractExeException;
import org.bit.core.exception.ContractValidateException;
import org.bit.core.store.AccountStore;
import org.bit.core.store.DelegatedResourceAccountIndexStore;
import org.bit.core.store.DelegatedResourceStore;
import org.bit.core.store.DynamicPropertiesStore;
import org.bit.protos.Protocol.Transaction.Contract.ContractType;
import org.bit.protos.Protocol.Transaction.Result.code;
import org.bit.protos.contract.BalanceContract.UnDelegateResourceContract;

@Slf4j(topic = "actuator")
public class UnDelegateResourceActuator extends AbstractActuator {

  public UnDelegateResourceActuator() {
    super(ContractType.UnDelegateResourceContract, UnDelegateResourceContract.class);
  }

  @Override
  public boolean execute(Object result) throws ContractExeException {
    TransactionResultCapsule ret = (TransactionResultCapsule) result;
    if (Objects.isNull(ret)) {
      throw new RuntimeException(ActuatorConstant.TX_RESULT_NULL);
    }

    long fee = calcFee();
    final UnDelegateResourceContract unDelegateResourceContract;
    AccountStore accountStore = chainBaseManager.getAccountStore();
    DynamicPropertiesStore dynamicStore = chainBaseManager.getDynamicPropertiesStore();
    DelegatedResourceStore delegatedResourceStore = chainBaseManager.getDelegatedResourceStore();
    DelegatedResourceAccountIndexStore delegatedResourceAccountIndexStore = chainBaseManager
        .getDelegatedResourceAccountIndexStore();
    try {
      unDelegateResourceContract = any.unpack(UnDelegateResourceContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      ret.setStatus(fee, code.FAILED);
      throw new ContractExeException(e.getMessage());
    }


    final BigInteger unDelegateBalance = BigIntegerUtil.newInstance(unDelegateResourceContract.getBalance());
    byte[] ownerAddress = unDelegateResourceContract.getOwnerAddress().toByteArray();
    byte[] receiverAddress = unDelegateResourceContract.getReceiverAddress().toByteArray();

    AccountCapsule receiverCapsule = accountStore.get(receiverAddress);

    long transferUsage = 0;
    // modify receiver Account
    if (receiverCapsule != null) {
      long now = chainBaseManager.getHeadSlot();
      switch (unDelegateResourceContract.getResource()) {
        case BANDWIDTH:
          BandwidthProcessor bandwidthProcessor = new BandwidthProcessor(chainBaseManager);
          bandwidthProcessor.updateUsageForDelegated(receiverCapsule);

          if (receiverCapsule.getAcquiredDelegatedFrozenV2BalanceForBandwidth().compareTo(unDelegateBalance) < 0) {
            // A TVM contract suicide, re-create will produce this situation
            receiverCapsule.setAcquiredDelegatedFrozenV2BalanceForBandwidth("0");
          } else {
            // calculate usage
            long unDelegateMaxUsage = (long) (unDelegateBalance.divide(BigInteger.valueOf(BIT_PRECISION)).doubleValue()
                * (BigInteger.valueOf(dynamicStore.getTotalNetLimit()).divide(dynamicStore.getTotalNetWeight())).doubleValue());
            transferUsage = (long) (receiverCapsule.getNetUsage()
                * (unDelegateBalance.divide(receiverCapsule.getAllFrozenBalanceForBandwidth()).doubleValue()));
            transferUsage = Math.min(unDelegateMaxUsage, transferUsage);

            receiverCapsule.addAcquiredDelegatedFrozenV2BalanceForBandwidth(unDelegateBalance.negate());
          }

          long newNetUsage = receiverCapsule.getNetUsage() - transferUsage;
          receiverCapsule.setNetUsage(newNetUsage);
          receiverCapsule.setLatestConsumeTime(now);
          break;
        case ENERGY:
          EnergyProcessor energyProcessor = new EnergyProcessor(dynamicStore, accountStore);
          energyProcessor.updateUsage(receiverCapsule);

          if (receiverCapsule.getAcquiredDelegatedFrozenV2BalanceForEnergy().compareTo(unDelegateBalance) < 0) {
            // A TVM contract receiver, re-create will produce this situation
            receiverCapsule.setAcquiredDelegatedFrozenV2BalanceForEnergy("0");
          } else {
            // calculate usage
            long unDelegateMaxUsage = (long) (unDelegateBalance.divide(BigInteger.valueOf(BIT_PRECISION)).doubleValue()
                * ((double) (dynamicStore.getTotalEnergyCurrentLimit()) / dynamicStore.getTotalEnergyWeight()));
            transferUsage = (long) (receiverCapsule.getEnergyUsage()
                * (unDelegateBalance.divide(receiverCapsule.getAllFrozenBalanceForEnergy()).doubleValue()));
            transferUsage = Math.min(unDelegateMaxUsage, transferUsage);

            receiverCapsule.addAcquiredDelegatedFrozenV2BalanceForEnergy(unDelegateBalance.negate());
          }

          long newEnergyUsage = receiverCapsule.getEnergyUsage() - transferUsage;
          receiverCapsule.setEnergyUsage(newEnergyUsage);
          receiverCapsule.setLatestConsumeTimeForEnergy(now);
          break;
        default:
          //this should never happen
          break;
      }
      accountStore.put(receiverCapsule.createDbKey(), receiverCapsule);
    }

    // transfer lock delegate to unlock
    delegatedResourceStore.unLockExpireResource(ownerAddress, receiverAddress,
        dynamicStore.getLatestBlockHeaderTimestamp());

    byte[] unlockKey = DelegatedResourceCapsule
        .createDbKeyV2(ownerAddress, receiverAddress, false);
    DelegatedResourceCapsule unlockResource = delegatedResourceStore
        .get(unlockKey);

    // modify owner Account
    AccountCapsule ownerCapsule = accountStore.get(ownerAddress);
    switch (unDelegateResourceContract.getResource()) {
      case BANDWIDTH: {
        unlockResource.addFrozenBalanceForBandwidth(unDelegateBalance.negate(), 0);

        ownerCapsule.addDelegatedFrozenV2BalanceForBandwidth(unDelegateBalance.negate());
        ownerCapsule.addFrozenBalanceForBandwidthV2(unDelegateBalance);

        BandwidthProcessor processor = new BandwidthProcessor(chainBaseManager);

        long now = chainBaseManager.getHeadSlot();
        if (Objects.nonNull(receiverCapsule) && transferUsage > 0) {
          processor.unDelegateIncrease(ownerCapsule, receiverCapsule,
              transferUsage, BANDWIDTH, now);
        }
      }
      break;
      case ENERGY: {
        unlockResource.addFrozenBalanceForEnergy(unDelegateBalance.negate(), 0);

        ownerCapsule.addDelegatedFrozenV2BalanceForEnergy(unDelegateBalance.negate());
        ownerCapsule.addFrozenBalanceForEnergyV2(unDelegateBalance);

        EnergyProcessor processor = new EnergyProcessor(dynamicStore, accountStore);

        long now = chainBaseManager.getHeadSlot();
        if (Objects.nonNull(receiverCapsule) && transferUsage > 0) {
          processor.unDelegateIncrease(ownerCapsule, receiverCapsule, transferUsage, ENERGY, now);
        }
      }
      break;
      default:
        //this should never happen
        break;
    }

    if (unlockResource.getFrozenBalanceForBandwidth().equals(BigInteger.ZERO)
        && unlockResource.getFrozenBalanceForEnergy().equals(BigInteger.ZERO)) {
      delegatedResourceStore.delete(unlockKey);
      unlockResource = null;
    } else {
      delegatedResourceStore.put(unlockKey, unlockResource);
    }

    byte[] lockKey = DelegatedResourceCapsule
        .createDbKeyV2(ownerAddress, receiverAddress, true);
    DelegatedResourceCapsule lockResource = delegatedResourceStore
        .get(lockKey);
    if (lockResource == null && unlockResource == null) {
      //modify DelegatedResourceAccountIndexStore
      delegatedResourceAccountIndexStore.unDelegateV2(ownerAddress, receiverAddress);
    }

    accountStore.put(ownerAddress, ownerCapsule);

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
    DelegatedResourceStore delegatedResourceStore = chainBaseManager.getDelegatedResourceStore();
    if (!dynamicStore.supportDR()) {
      throw new ContractValidateException("No support for resource delegate");
    }

    if (!dynamicStore.supportUnfreezeDelay()) {
      throw new ContractValidateException("Not support unDelegate resource transaction,"
          + " need to be opened by the committee");
    }

    if (!this.any.is(UnDelegateResourceContract.class)) {
      throw new ContractValidateException(
          "contract type error, expected type [UnDelegateResourceContract], real type[" + any
              .getClass() + "]");
    }
    final UnDelegateResourceContract unDelegateResourceContract;
    try {
      unDelegateResourceContract = this.any.unpack(UnDelegateResourceContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }

    byte[] ownerAddress = unDelegateResourceContract.getOwnerAddress().toByteArray();
    if (!DecodeUtil.addressValid(ownerAddress)) {
      throw new ContractValidateException("Invalid address");
    }
    AccountCapsule ownerCapsule = accountStore.get(ownerAddress);
    if (ownerCapsule == null) {
      String readableOwnerAddress = StringUtil.createReadableString(ownerAddress);
      throw new ContractValidateException(
          ACCOUNT_EXCEPTION_STR + readableOwnerAddress + "] does not exist");
    }

    byte[] receiverAddress = unDelegateResourceContract.getReceiverAddress().toByteArray();
    if (!DecodeUtil.addressValid(receiverAddress)) {
      throw new ContractValidateException("Invalid receiverAddress");
    }
    if (Arrays.equals(receiverAddress, ownerAddress)) {
      throw new ContractValidateException(
          "receiverAddress must not be the same as ownerAddress");
    }

    // TVM contract suicide can result in no receiving account
    // AccountCapsule receiverCapsule = accountStore.get(receiverAddress);
    // if (receiverCapsule == null) {
    //   String readableReceiverAddress = StringUtil.createReadableString(receiverAddress);
    //   throw new ContractValidateException(
    //       "Receiver Account[" + readableReceiverAddress + "] does not exist");
    // }

    long now = dynamicStore.getLatestBlockHeaderTimestamp();
    byte[] key = DelegatedResourceCapsule.createDbKeyV2(ownerAddress, receiverAddress, false);
    DelegatedResourceCapsule unlockResourceCapsule = delegatedResourceStore.get(key);
    byte[] lockKey = DelegatedResourceCapsule.createDbKeyV2(ownerAddress, receiverAddress, true);
    DelegatedResourceCapsule lockResourceCapsule = delegatedResourceStore.get(lockKey);
    if (unlockResourceCapsule == null && lockResourceCapsule == null) {
      throw new ContractValidateException(
          "delegated Resource does not exist");
    }

    BigInteger unDelegateBalance = BigIntegerUtil.newInstance(unDelegateResourceContract.getBalance());
    if (unDelegateBalance.compareTo(BigInteger.ZERO) <= 0) {
      throw new ContractValidateException("unDelegateBalance must be more than 0 BIT");
    }
    switch (unDelegateResourceContract.getResource()) {
      case BANDWIDTH: {
        BigInteger delegateBalance = BigInteger.ZERO;
        if (unlockResourceCapsule != null) {
          delegateBalance = delegateBalance.add(unlockResourceCapsule.getFrozenBalanceForBandwidth());
        }
        if (lockResourceCapsule != null
            && lockResourceCapsule.getExpireTimeForBandwidth() < now) {
          delegateBalance = delegateBalance.add(lockResourceCapsule.getFrozenBalanceForBandwidth());
        }
        if (delegateBalance.compareTo(unDelegateBalance) < 0) {
          throw new ContractValidateException(
              "insufficient delegatedFrozenBalance(BANDWIDTH), request="
                  + unDelegateBalance + ", unlock_balance=" + delegateBalance);
        }
      }
      break;
      case ENERGY: {
        BigInteger delegateBalance = BigInteger.ZERO;
        if (unlockResourceCapsule != null) {
          delegateBalance = delegateBalance.add(unlockResourceCapsule.getFrozenBalanceForEnergy());
        }
        if (lockResourceCapsule != null
            && lockResourceCapsule.getExpireTimeForEnergy() < now) {
          delegateBalance = delegateBalance.add(lockResourceCapsule.getFrozenBalanceForEnergy());
        }
        if (delegateBalance.compareTo(unDelegateBalance) < 0) {
          throw new ContractValidateException("insufficient delegateFrozenBalance(Energy), request="
              + unDelegateBalance + ", unlock_balance=" + delegateBalance);
        }
      }
      break;
      default:
        throw new ContractValidateException(
            "ResourceCode error.valid ResourceCode[BANDWIDTH、Energy]");
    }

    return true;
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return any.unpack(UnDelegateResourceContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return 0;
  }

}
