package org.bit.core.actuator;

import static org.bit.core.actuator.ActuatorConstant.ACCOUNT_EXCEPTION_STR;
import static org.bit.core.config.Parameter.ChainConstant.BIT_PRECISION;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.bit.common.utils.BigIntegerUtil;
import org.bit.common.utils.DecodeUtil;
import org.bit.common.utils.StringUtil;
import org.bit.core.capsule.AccountCapsule;
import org.bit.core.capsule.DelegatedResourceAccountIndexCapsule;
import org.bit.core.capsule.DelegatedResourceCapsule;
import org.bit.core.capsule.TransactionResultCapsule;
import org.bit.core.capsule.VotesCapsule;
import org.bit.core.exception.ContractExeException;
import org.bit.core.exception.ContractValidateException;
import org.bit.core.service.MortgageService;
import org.bit.core.store.AccountStore;
import org.bit.core.store.DelegatedResourceAccountIndexStore;
import org.bit.core.store.DelegatedResourceStore;
import org.bit.core.store.DynamicPropertiesStore;
import org.bit.core.store.VotesStore;
import org.bit.protos.Protocol.Account.AccountResource;
import org.bit.protos.Protocol.Account.Frozen;
import org.bit.protos.Protocol.AccountType;
import org.bit.protos.Protocol.Transaction.Contract.ContractType;
import org.bit.protos.Protocol.Transaction.Result.code;
import org.bit.protos.contract.BalanceContract.UnfreezeBalanceContract;

@Slf4j(topic = "actuator")
public class UnfreezeBalanceActuator extends AbstractActuator {

  private static final String INVALID_RESOURCE_CODE =
          "ResourceCode error.valid ResourceCode[BANDWIDTH、Energy]";

  public UnfreezeBalanceActuator() {
    super(ContractType.UnfreezeBalanceContract, UnfreezeBalanceContract.class);
  }

  @Override
  public boolean execute(Object result) throws ContractExeException {
    TransactionResultCapsule ret = (TransactionResultCapsule) result;
    if (Objects.isNull(ret)) {
      throw new RuntimeException(ActuatorConstant.TX_RESULT_NULL);
    }

    long fee = calcFee();
    final UnfreezeBalanceContract unfreezeBalanceContract;
    AccountStore accountStore = chainBaseManager.getAccountStore();
    DynamicPropertiesStore dynamicStore = chainBaseManager.getDynamicPropertiesStore();
    DelegatedResourceStore delegatedResourceStore = chainBaseManager.getDelegatedResourceStore();
    DelegatedResourceAccountIndexStore delegatedResourceAccountIndexStore = chainBaseManager
        .getDelegatedResourceAccountIndexStore();
    VotesStore votesStore = chainBaseManager.getVotesStore();
    MortgageService mortgageService = chainBaseManager.getMortgageService();
    try {
      unfreezeBalanceContract = any.unpack(UnfreezeBalanceContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      ret.setStatus(fee, code.FAILED);
      throw new ContractExeException(e.getMessage());
    }
    byte[] ownerAddress = unfreezeBalanceContract.getOwnerAddress().toByteArray();

    //
    mortgageService.withdrawReward(ownerAddress);

    AccountCapsule accountCapsule = accountStore.get(ownerAddress);
    BigInteger oldBalance = accountCapsule.getBalance();

    BigInteger unfreezeBalance = BigInteger.ZERO;

    if (dynamicStore.supportAllowNewResourceModel()
        && accountCapsule.oldBitPowerIsNotInitialized()) {
      accountCapsule.initializeOldBitPower();
    }

    byte[] receiverAddress = unfreezeBalanceContract.getReceiverAddress().toByteArray();
    //If the receiver is not included in the contract, unfreeze frozen balance for this account.
    //otherwise,unfreeze delegated frozen balance provided this account.
    long decrease = 0;
    if (!ArrayUtils.isEmpty(receiverAddress) && dynamicStore.supportDR()) {
      byte[] key = DelegatedResourceCapsule
          .createDbKey(unfreezeBalanceContract.getOwnerAddress().toByteArray(),
              unfreezeBalanceContract.getReceiverAddress().toByteArray());
      DelegatedResourceCapsule delegatedResourceCapsule = delegatedResourceStore
          .get(key);

      switch (unfreezeBalanceContract.getResource()) {
        case BANDWIDTH:
          unfreezeBalance = delegatedResourceCapsule.getFrozenBalanceForBandwidth();
          delegatedResourceCapsule.setFrozenBalanceForBandwidth("0", 0);
          accountCapsule.addDelegatedFrozenBalanceForBandwidth(unfreezeBalance.negate());
          break;
        case ENERGY:
          unfreezeBalance = delegatedResourceCapsule.getFrozenBalanceForEnergy();
          delegatedResourceCapsule.setFrozenBalanceForEnergy("0", 0);
          accountCapsule.addDelegatedFrozenBalanceForEnergy(unfreezeBalance.negate());
          break;
        default:
          //this should never happen
          break;
      }

      AccountCapsule receiverCapsule = accountStore.get(receiverAddress);

      if (dynamicStore.getAllowTvmConstantinople() == 0 ||
          (receiverCapsule != null && receiverCapsule.getType() != AccountType.Contract)) {
        switch (unfreezeBalanceContract.getResource()) {
          case BANDWIDTH:
            long oldNetWeight = receiverCapsule.getAcquiredDelegatedFrozenBalanceForBandwidth().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
            if (dynamicStore.getAllowTvmSolidity059() == 1
                && receiverCapsule.getAcquiredDelegatedFrozenBalanceForBandwidth().compareTo(unfreezeBalance)
                < 0) {
              oldNetWeight = unfreezeBalance.divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
              receiverCapsule.setAcquiredDelegatedFrozenBalanceForBandwidth("0");
            } else {
              receiverCapsule.addAcquiredDelegatedFrozenBalanceForBandwidth(unfreezeBalance.negate());
            }
            long newNetWeight = receiverCapsule.getAcquiredDelegatedFrozenBalanceForBandwidth().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
            decrease = newNetWeight - oldNetWeight;
            break;
          case ENERGY:
            long oldEnergyWeight = receiverCapsule.getAcquiredDelegatedFrozenBalanceForEnergy().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
            if (dynamicStore.getAllowTvmSolidity059() == 1
                && receiverCapsule.getAcquiredDelegatedFrozenBalanceForEnergy().compareTo(unfreezeBalance) < 0) {
              oldEnergyWeight = unfreezeBalance.divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
              receiverCapsule.setAcquiredDelegatedFrozenBalanceForEnergy("0");
            } else {
              receiverCapsule.addAcquiredDelegatedFrozenBalanceForEnergy(unfreezeBalance.negate());
            }
            long newEnergyWeight = receiverCapsule.getAcquiredDelegatedFrozenBalanceForEnergy().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
            decrease = newEnergyWeight - oldEnergyWeight;
            break;
          default:
            //this should never happen
            break;
        }
        accountStore.put(receiverCapsule.createDbKey(), receiverCapsule);
      } else {
        decrease = unfreezeBalance.negate().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
      }

      accountCapsule.setBalance(oldBalance.add(unfreezeBalance).toString());

      if (delegatedResourceCapsule.getFrozenBalanceForBandwidth().equals(BigInteger.ZERO)
          && delegatedResourceCapsule.getFrozenBalanceForEnergy().equals(BigInteger.ZERO)) {
        delegatedResourceStore.delete(key);

        //modify DelegatedResourceAccountIndexStore
        if (!dynamicStore.supportAllowDelegateOptimization()) {
          DelegatedResourceAccountIndexCapsule ownerIndexCapsule =
              delegatedResourceAccountIndexStore.get(ownerAddress);
          if (ownerIndexCapsule != null) {
            List<ByteString> toAccountsList = new ArrayList<>(ownerIndexCapsule
                .getToAccountsList());
            toAccountsList.remove(ByteString.copyFrom(receiverAddress));
            ownerIndexCapsule.setAllToAccounts(toAccountsList);
            delegatedResourceAccountIndexStore.put(ownerAddress, ownerIndexCapsule);
          }

          DelegatedResourceAccountIndexCapsule receiverIndexCapsule =
              delegatedResourceAccountIndexStore.get(receiverAddress);
          if (receiverIndexCapsule != null) {
            List<ByteString> fromAccountsList = new ArrayList<>(receiverIndexCapsule
                .getFromAccountsList());
            fromAccountsList.remove(ByteString.copyFrom(ownerAddress));
            receiverIndexCapsule.setAllFromAccounts(fromAccountsList);
            delegatedResourceAccountIndexStore.put(receiverAddress, receiverIndexCapsule);
          }
        } else {
          //modify DelegatedResourceAccountIndexStore new
          delegatedResourceAccountIndexStore.convert(ownerAddress);
          delegatedResourceAccountIndexStore.convert(receiverAddress);
          delegatedResourceAccountIndexStore.unDelegate(ownerAddress, receiverAddress);
        }
      } else {
        delegatedResourceStore.put(key, delegatedResourceCapsule);
      }
    } else {
      switch (unfreezeBalanceContract.getResource()) {
        case BANDWIDTH:
          long oldNetWeight = accountCapsule.getFrozenBalance().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
          List<Frozen> frozenList = Lists.newArrayList();
          frozenList.addAll(accountCapsule.getFrozenList());
          Iterator<Frozen> iterator = frozenList.iterator();
          long now = dynamicStore.getLatestBlockHeaderTimestamp();
          while (iterator.hasNext()) {
            Frozen next = iterator.next();
            if (next.getExpireTime() <= now) {
              unfreezeBalance = unfreezeBalance.add(BigIntegerUtil.newInstance(next.getFrozenBalance()));
              iterator.remove();
            }
          }

          accountCapsule.setInstance(accountCapsule.getInstance().toBuilder()
              .setBalance(oldBalance.add(unfreezeBalance).toString())
              .clearFrozen().addAllFrozen(frozenList).build());
          long newNetWeight = accountCapsule.getFrozenBalance().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
          decrease = newNetWeight - oldNetWeight;
          break;
        case ENERGY:
          long oldEnergyWeight = accountCapsule.getEnergyFrozenBalance().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
          unfreezeBalance = BigIntegerUtil.newInstance(accountCapsule.getAccountResource().getFrozenBalanceForEnergy().getFrozenBalance());

          AccountResource newAccountResource = accountCapsule.getAccountResource().toBuilder()
              .clearFrozenBalanceForEnergy().build();
          accountCapsule.setInstance(accountCapsule.getInstance().toBuilder()
              .setBalance(oldBalance.add(unfreezeBalance).toString())
              .setAccountResource(newAccountResource).build());
          long newEnergyWeight = accountCapsule.getEnergyFrozenBalance().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
          decrease = newEnergyWeight - oldEnergyWeight;
          break;
        case BIT_POWER:
          long oldTPWeight = accountCapsule.getBitPowerFrozenBalance().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
          unfreezeBalance = accountCapsule.getBitPowerFrozenBalance();
          accountCapsule.setInstance(accountCapsule.getInstance().toBuilder()
              .setBalance(oldBalance.add(unfreezeBalance).toString())
              .clearBitPower().build());
          long newTPWeight = accountCapsule.getBitPowerFrozenBalance().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
          decrease = newTPWeight - oldTPWeight;
          break;
        default:
          //this should never happen
          break;
      }

    }
    long weight = dynamicStore.allowNewReward() ? decrease : unfreezeBalance.negate().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
    switch (unfreezeBalanceContract.getResource()) {
      case BANDWIDTH:
        dynamicStore
            .addTotalNetWeight(BigInteger.valueOf(weight));
        break;
      case ENERGY:
        dynamicStore
            .addTotalEnergyWeight(weight);
        break;
      case BIT_POWER:
        dynamicStore
            .addTotalBitPowerWeight(weight);
        break;
      default:
        //this should never happen
        break;
    }

    boolean needToClearVote = true;
    if (dynamicStore.supportAllowNewResourceModel()
        && accountCapsule.oldBitPowerIsInvalid()) {
      switch (unfreezeBalanceContract.getResource()) {
        case BANDWIDTH:
        case ENERGY:
          needToClearVote = false;
          break;
        default:
          break;
      }
    }

    if (needToClearVote) {
      VotesCapsule votesCapsule;
      if (!votesStore.has(ownerAddress)) {
        votesCapsule = new VotesCapsule(unfreezeBalanceContract.getOwnerAddress(),
            accountCapsule.getVotesList());
      } else {
        votesCapsule = votesStore.get(ownerAddress);
      }
      accountCapsule.clearVotes();
      votesCapsule.clearNewVotes();
      votesStore.put(ownerAddress, votesCapsule);
    }

    if (dynamicStore.supportAllowNewResourceModel()
        && !accountCapsule.oldBitPowerIsInvalid()) {
      accountCapsule.invalidateOldBitPower();
    }

    accountStore.put(ownerAddress, accountCapsule);

    ret.setUnfreezeAmount(unfreezeBalance.toString());
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
    if (!this.any.is(UnfreezeBalanceContract.class)) {
      throw new ContractValidateException(
          "contract type error, expected type [UnfreezeBalanceContract], real type[" + any
              .getClass() + "]");
    }
    final UnfreezeBalanceContract unfreezeBalanceContract;
    try {
      unfreezeBalanceContract = this.any.unpack(UnfreezeBalanceContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }
    byte[] ownerAddress = unfreezeBalanceContract.getOwnerAddress().toByteArray();
    if (!DecodeUtil.addressValid(ownerAddress)) {
      throw new ContractValidateException("Invalid address");
    }

    AccountCapsule accountCapsule = accountStore.get(ownerAddress);
    if (accountCapsule == null) {
      String readableOwnerAddress = StringUtil.createReadableString(ownerAddress);
      throw new ContractValidateException(
          ACCOUNT_EXCEPTION_STR + readableOwnerAddress + "] does not exist");
    }
    long now = dynamicStore.getLatestBlockHeaderTimestamp();
    byte[] receiverAddress = unfreezeBalanceContract.getReceiverAddress().toByteArray();
    //If the receiver is not included in the contract, unfreeze frozen balance for this account.
    //otherwise,unfreeze delegated frozen balance provided this account.
    if (!ArrayUtils.isEmpty(receiverAddress) && dynamicStore.supportDR()) {
      if (Arrays.equals(receiverAddress, ownerAddress)) {
        throw new ContractValidateException(
            "receiverAddress must not be the same as ownerAddress");
      }

      if (!DecodeUtil.addressValid(receiverAddress)) {
        throw new ContractValidateException("Invalid receiverAddress");
      }

      AccountCapsule receiverCapsule = accountStore.get(receiverAddress);
      if (dynamicStore.getAllowTvmConstantinople() == 0
          && receiverCapsule == null) {
        String readableReceiverAddress = StringUtil.createReadableString(receiverAddress);
        throw new ContractValidateException(
            "Receiver Account[" + readableReceiverAddress + "] does not exist");
      }

      byte[] key = DelegatedResourceCapsule
          .createDbKey(unfreezeBalanceContract.getOwnerAddress().toByteArray(),
              unfreezeBalanceContract.getReceiverAddress().toByteArray());
      DelegatedResourceCapsule delegatedResourceCapsule = delegatedResourceStore
          .get(key);
      if (delegatedResourceCapsule == null) {
        throw new ContractValidateException(
            "delegated Resource does not exist");
      }

      switch (unfreezeBalanceContract.getResource()) {
        case BANDWIDTH:
          if (delegatedResourceCapsule.getFrozenBalanceForBandwidth().compareTo(BigInteger.ZERO) <= 0) {
            throw new ContractValidateException("no delegatedFrozenBalance(BANDWIDTH)");
          }

          if (dynamicStore.getAllowTvmConstantinople() == 0) {
            if (receiverCapsule.getAcquiredDelegatedFrozenBalanceForBandwidth().compareTo(delegatedResourceCapsule.getFrozenBalanceForBandwidth()) < 0) {
              throw new ContractValidateException(
                  "AcquiredDelegatedFrozenBalanceForBandwidth[" + receiverCapsule
                      .getAcquiredDelegatedFrozenBalanceForBandwidth() + "] < delegatedBandwidth["
                      + delegatedResourceCapsule.getFrozenBalanceForBandwidth()
                      + "]");
            }
          } else {
            if (dynamicStore.getAllowTvmSolidity059() != 1
                && receiverCapsule != null
                && receiverCapsule.getType() != AccountType.Contract
                && receiverCapsule.getAcquiredDelegatedFrozenBalanceForBandwidth().compareTo(delegatedResourceCapsule.getFrozenBalanceForBandwidth()) < 0) {
              throw new ContractValidateException(
                  "AcquiredDelegatedFrozenBalanceForBandwidth[" + receiverCapsule
                      .getAcquiredDelegatedFrozenBalanceForBandwidth() + "] < delegatedBandwidth["
                      + delegatedResourceCapsule.getFrozenBalanceForBandwidth()
                      + "]");
            }
          }

          if (delegatedResourceCapsule.getExpireTimeForBandwidth() > now) {
            throw new ContractValidateException("It's not time to unfreeze.");
          }
          break;
        case ENERGY:
          if (delegatedResourceCapsule.getFrozenBalanceForEnergy().compareTo(BigInteger.ZERO) <= 0) {
            throw new ContractValidateException("no delegateFrozenBalance(Energy)");
          }
          if (dynamicStore.getAllowTvmConstantinople() == 0) {
            if (receiverCapsule.getAcquiredDelegatedFrozenBalanceForEnergy().compareTo(delegatedResourceCapsule.getFrozenBalanceForEnergy()) < 0) {
              throw new ContractValidateException(
                  "AcquiredDelegatedFrozenBalanceForEnergy[" + receiverCapsule
                      .getAcquiredDelegatedFrozenBalanceForEnergy() + "] < delegatedEnergy["
                      + delegatedResourceCapsule.getFrozenBalanceForEnergy() +
                      "]");
            }
          } else {
            if (dynamicStore.getAllowTvmSolidity059() != 1
                && receiverCapsule != null
                && receiverCapsule.getType() != AccountType.Contract
                && receiverCapsule.getAcquiredDelegatedFrozenBalanceForEnergy().compareTo(delegatedResourceCapsule.getFrozenBalanceForEnergy()) < 0) {
              throw new ContractValidateException(
                  "AcquiredDelegatedFrozenBalanceForEnergy[" + receiverCapsule
                      .getAcquiredDelegatedFrozenBalanceForEnergy() + "] < delegatedEnergy["
                      + delegatedResourceCapsule.getFrozenBalanceForEnergy() +
                      "]");
            }
          }

          if (delegatedResourceCapsule.getExpireTimeForEnergy(dynamicStore) > now) {
            throw new ContractValidateException("It's not time to unfreeze.");
          }
          break;
        default:
          throw new ContractValidateException(INVALID_RESOURCE_CODE);
      }

    } else {
      switch (unfreezeBalanceContract.getResource()) {
        case BANDWIDTH:
          if (accountCapsule.getFrozenCount() <= 0) {
            throw new ContractValidateException("no frozenBalance(BANDWIDTH)");
          }

          long allowedUnfreezeCount = accountCapsule.getFrozenList().stream()
              .filter(frozen -> frozen.getExpireTime() <= now).count();
          if (allowedUnfreezeCount <= 0) {
            throw new ContractValidateException("It's not time to unfreeze(BANDWIDTH).");
          }
          break;
        case ENERGY:
          Frozen frozenBalanceForEnergy = accountCapsule.getAccountResource()
              .getFrozenBalanceForEnergy();
          if (BigIntegerUtil.newInstance(frozenBalanceForEnergy.getFrozenBalance()).compareTo(BigInteger.ZERO) <= 0) {
            throw new ContractValidateException("no frozenBalance(Energy)");
          }
          if (frozenBalanceForEnergy.getExpireTime() > now) {
            throw new ContractValidateException("It's not time to unfreeze(Energy).");
          }

          break;
        case BIT_POWER:
          if (dynamicStore.supportAllowNewResourceModel()) {
            Frozen frozenBalanceForBitPower = accountCapsule.getInstance().getBitPower();
            if (BigIntegerUtil.newInstance(frozenBalanceForBitPower.getFrozenBalance()).compareTo(BigInteger.ZERO) <= 0) {
              throw new ContractValidateException("no frozenBalance(BitPower)");
            }
            if (frozenBalanceForBitPower.getExpireTime() > now) {
              throw new ContractValidateException("It's not time to unfreeze(BitPower).");
            }
          } else {
            throw new ContractValidateException(INVALID_RESOURCE_CODE);
          }
          break;
        default:
          if (dynamicStore.supportAllowNewResourceModel()) {
            throw new ContractValidateException(
                "ResourceCode error.valid ResourceCode[BANDWIDTH、Energy、BIT_POWER]");
          } else {
            throw new ContractValidateException(INVALID_RESOURCE_CODE);
          }
      }

    }

    return true;
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return any.unpack(UnfreezeBalanceContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return 0;
  }

}
