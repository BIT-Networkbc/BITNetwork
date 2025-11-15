package org.bit.core.actuator;

import static org.bit.core.actuator.ActuatorConstant.ACCOUNT_EXCEPTION_STR;
import static org.bit.core.config.Parameter.ChainConstant.FROZEN_PERIOD;
import static org.bit.core.config.Parameter.ChainConstant.BIT_PRECISION;
import static org.bit.protos.contract.Common.ResourceCode;
import static org.bit.protos.contract.Common.ResourceCode.BANDWIDTH;
import static org.bit.protos.contract.Common.ResourceCode.ENERGY;
import static org.bit.protos.contract.Common.ResourceCode.BIT_POWER;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.bit.common.utils.BigIntegerUtil;
import org.bit.common.utils.DecodeUtil;
import org.bit.common.utils.StringUtil;
import org.bit.core.capsule.AccountCapsule;
import org.bit.core.capsule.TransactionResultCapsule;
import org.bit.core.capsule.VotesCapsule;
import org.bit.core.exception.ContractExeException;
import org.bit.core.exception.ContractValidateException;
import org.bit.core.service.MortgageService;
import org.bit.core.store.AccountStore;
import org.bit.core.store.DynamicPropertiesStore;
import org.bit.core.store.VotesStore;
import org.bit.protos.Protocol;
import org.bit.protos.Protocol.Account.FreezeV2;
import org.bit.protos.Protocol.Account.UnFreezeV2;
import org.bit.protos.Protocol.Transaction.Contract.ContractType;
import org.bit.protos.Protocol.Transaction.Result.code;
import org.bit.protos.Protocol.Vote;
import org.bit.protos.contract.BalanceContract.UnfreezeBalanceV2Contract;

@Slf4j(topic = "actuator")
public class UnfreezeBalanceV2Actuator extends AbstractActuator {

  @Getter
  private static final int UNFREEZE_MAX_TIMES = 32;

  public UnfreezeBalanceV2Actuator() {
    super(ContractType.UnfreezeBalanceV2Contract, UnfreezeBalanceV2Contract.class);
  }

  @Override
  public boolean execute(Object result) throws ContractExeException {
    TransactionResultCapsule ret = (TransactionResultCapsule) result;
    if (Objects.isNull(ret)) {
      throw new RuntimeException(ActuatorConstant.TX_RESULT_NULL);
    }

    long fee = calcFee();
    final UnfreezeBalanceV2Contract unfreezeBalanceV2Contract;
    AccountStore accountStore = chainBaseManager.getAccountStore();
    DynamicPropertiesStore dynamicStore = chainBaseManager.getDynamicPropertiesStore();
    MortgageService mortgageService = chainBaseManager.getMortgageService();
    try {
      unfreezeBalanceV2Contract = any.unpack(UnfreezeBalanceV2Contract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      ret.setStatus(fee, code.FAILED);
      throw new ContractExeException(e.getMessage());
    }
    byte[] ownerAddress = unfreezeBalanceV2Contract.getOwnerAddress().toByteArray();
    long now = dynamicStore.getLatestBlockHeaderTimestamp();

    mortgageService.withdrawReward(ownerAddress);

    AccountCapsule accountCapsule = accountStore.get(ownerAddress);
    BigInteger unfreezeAmount = this.unfreezeExpire(accountCapsule, now);
    BigInteger unfreezeBalance = BigIntegerUtil.newInstance(unfreezeBalanceV2Contract.getUnfreezeBalance());

    if (dynamicStore.supportAllowNewResourceModel()
        && accountCapsule.oldBitPowerIsNotInitialized()) {
      accountCapsule.initializeOldBitPower();
    }

    ResourceCode freezeType = unfreezeBalanceV2Contract.getResource();

    long expireTime = this.calcUnfreezeExpireTime(now);
    accountCapsule.addUnfrozenV2List(freezeType, String.valueOf(unfreezeBalance), expireTime);

    this.updateTotalResourceWeight(accountCapsule, unfreezeBalanceV2Contract, unfreezeBalance);
    this.updateVote(accountCapsule, unfreezeBalanceV2Contract, ownerAddress);

    if (dynamicStore.supportAllowNewResourceModel()
        && !accountCapsule.oldBitPowerIsInvalid()) {
      accountCapsule.invalidateOldBitPower();
    }

    accountStore.put(ownerAddress, accountCapsule);

    ret.setWithdrawExpireAmount(String.valueOf(unfreezeAmount));
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
    if (!this.any.is(UnfreezeBalanceV2Contract.class)) {
      throw new ContractValidateException(
          "contract type error, expected type [UnfreezeBalanceContract], real type[" + any
              .getClass() + "]");
    }

    if (!dynamicStore.supportUnfreezeDelay()) {
      throw new ContractValidateException("Not support UnfreezeV2 transaction,"
          + " need to be opened by the committee");
    }

    final UnfreezeBalanceV2Contract unfreezeBalanceV2Contract;
    try {
      unfreezeBalanceV2Contract = this.any.unpack(UnfreezeBalanceV2Contract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }

    byte[] ownerAddress = unfreezeBalanceV2Contract.getOwnerAddress().toByteArray();
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
    switch (unfreezeBalanceV2Contract.getResource()) {
      case BANDWIDTH:
        if (!checkExistFrozenBalance(accountCapsule, BANDWIDTH)) {
          throw new ContractValidateException("no frozenBalance(BANDWIDTH)");
        }
        break;
      case ENERGY:
        if (!checkExistFrozenBalance(accountCapsule, ENERGY)) {
          throw new ContractValidateException("no frozenBalance(Energy)");
        }
        break;
      case BIT_POWER:
        if (dynamicStore.supportAllowNewResourceModel()) {
          if (!checkExistFrozenBalance(accountCapsule, BIT_POWER)) {
            throw new ContractValidateException("no frozenBalance(BitPower)");
          }
        } else {
          throw new ContractValidateException("ResourceCode error.valid ResourceCode[BANDWIDTH、Energy]");
        }
        break;
      default:
        if (dynamicStore.supportAllowNewResourceModel()) {
          throw new ContractValidateException("ResourceCode error.valid ResourceCode[BANDWIDTH、Energy、BIT_POWER]");
        } else {
          throw new ContractValidateException("ResourceCode error.valid ResourceCode[BANDWIDTH、Energy]");
        }
    }

    if (!checkUnfreezeBalance(accountCapsule, unfreezeBalanceV2Contract, unfreezeBalanceV2Contract.getResource())) {
      throw new ContractValidateException(
          "Invalid unfreeze_balance, [" + unfreezeBalanceV2Contract.getUnfreezeBalance() + "] is error"
      );
    }

    int unfreezingCount = accountCapsule.getUnfreezingV2Count(now);
    if (UNFREEZE_MAX_TIMES <= unfreezingCount) {
      throw new ContractValidateException("Invalid unfreeze operation, unfreezing times is over limit");
    }

    return true;
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return any.unpack(UnfreezeBalanceV2Contract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return 0;
  }

  public boolean checkExistFrozenBalance(AccountCapsule accountCapsule, ResourceCode freezeType) {
    List<FreezeV2> frozenV2List = accountCapsule.getFrozenV2List();
    for (FreezeV2 frozenV2 : frozenV2List) {
      if (frozenV2.getType().equals(freezeType) && BigIntegerUtil.newInstance(frozenV2.getAmount()).compareTo(BigInteger.ZERO) > 0) {
        return true;
      }
    }
    return false;
  }

  public boolean checkUnfreezeBalance(AccountCapsule accountCapsule,
                                      final UnfreezeBalanceV2Contract unfreezeBalanceV2Contract,
                                      ResourceCode freezeType) {
    boolean checkOk = false;

    BigInteger frozenAmount = BigInteger.ZERO;
    List<FreezeV2> freezeV2List = accountCapsule.getFrozenV2List();
    for (FreezeV2 freezeV2 : freezeV2List) {
      if (freezeV2.getType().equals(freezeType)) {
        frozenAmount = BigIntegerUtil.newInstance(freezeV2.getAmount());
        break;
      }
    }

    if (BigIntegerUtil.newInstance(unfreezeBalanceV2Contract.getUnfreezeBalance()).compareTo(BigInteger.ZERO) > 0
        && BigIntegerUtil.newInstance(unfreezeBalanceV2Contract.getUnfreezeBalance()).compareTo(frozenAmount) <= 0) {
      checkOk = true;
    }

    return checkOk;
  }

  public long calcUnfreezeExpireTime(long now) {
    DynamicPropertiesStore dynamicStore = chainBaseManager.getDynamicPropertiesStore();
    long unfreezeDelayDays = dynamicStore.getUnfreezeDelayDays();

    return now + unfreezeDelayDays * FROZEN_PERIOD;
  }

  public void updateAccountFrozenInfo(ResourceCode freezeType, AccountCapsule accountCapsule, BigInteger unfreezeBalance) {
    List<FreezeV2> freezeV2List = accountCapsule.getFrozenV2List();
    for (int i = 0; i < freezeV2List.size(); i++) {
      if (freezeV2List.get(i).getType().equals(freezeType)) {
        FreezeV2 freezeV2 = FreezeV2.newBuilder()
            .setAmount(BigIntegerUtil.newInstance(freezeV2List.get(i).getAmount()).subtract(unfreezeBalance).toString())
            .setType(freezeV2List.get(i).getType())
            .build();
        accountCapsule.updateFrozenV2List(i, freezeV2);
        break;
      }
    }
  }

  public BigInteger unfreezeExpire(AccountCapsule accountCapsule, long now) {
    BigInteger unfreezeBalance = BigInteger.ZERO;

    List<UnFreezeV2> unFrozenV2List = Lists.newArrayList();
    unFrozenV2List.addAll(accountCapsule.getUnfrozenV2List());
    Iterator<UnFreezeV2> iterator = unFrozenV2List.iterator();

    while (iterator.hasNext()) {
      UnFreezeV2 next = iterator.next();
      if (next.getUnfreezeExpireTime() <= now) {
        unfreezeBalance = unfreezeBalance.add(BigIntegerUtil.newInstance(next.getUnfreezeAmount()));
        iterator.remove();
      }
    }

    accountCapsule.setInstance(
        accountCapsule.getInstance().toBuilder()
            .setBalance(accountCapsule.getBalance().add(unfreezeBalance).toString())
            .clearUnfrozenV2()
            .addAllUnfrozenV2(unFrozenV2List).build()
    );
    return unfreezeBalance;
  }

  public void updateTotalResourceWeight(AccountCapsule accountCapsule,
                                        final UnfreezeBalanceV2Contract unfreezeBalanceV2Contract,
                                        BigInteger unfreezeBalance) {
    DynamicPropertiesStore dynamicStore = chainBaseManager.getDynamicPropertiesStore();
    switch (unfreezeBalanceV2Contract.getResource()) {
      case BANDWIDTH:
        long oldNetWeight = accountCapsule.getFrozenV2BalanceWithDelegated(BANDWIDTH).divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        accountCapsule.addFrozenBalanceForBandwidthV2(unfreezeBalance.negate());
        long newNetWeight = accountCapsule.getFrozenV2BalanceWithDelegated(BANDWIDTH).divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        dynamicStore.addTotalNetWeight(BigInteger.valueOf(newNetWeight - oldNetWeight));
        break;
      case ENERGY:
        long oldEnergyWeight = accountCapsule.getFrozenV2BalanceWithDelegated(ENERGY).divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        accountCapsule.addFrozenBalanceForEnergyV2(unfreezeBalance.negate());
        long newEnergyWeight = accountCapsule.getFrozenV2BalanceWithDelegated(ENERGY).divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        dynamicStore.addTotalEnergyWeight(newEnergyWeight - oldEnergyWeight);
        break;
      case BIT_POWER:
        long oldTPWeight = accountCapsule.getBitPowerFrozenV2Balance().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        accountCapsule.addFrozenForBitPowerV2(unfreezeBalance.negate());
        long newTPWeight = accountCapsule.getBitPowerFrozenV2Balance().divide(BigInteger.valueOf(BIT_PRECISION)).longValue();
        dynamicStore.addTotalBitPowerWeight(newTPWeight - oldTPWeight);
        break;
      default:
        //this should never happen
        break;
    }
  }

  private void updateVote(AccountCapsule accountCapsule,
                          final UnfreezeBalanceV2Contract unfreezeBalanceV2Contract,
                          byte[] ownerAddress) {
    DynamicPropertiesStore dynamicStore = chainBaseManager.getDynamicPropertiesStore();
    VotesStore votesStore = chainBaseManager.getVotesStore();

    if (accountCapsule.getVotesList().isEmpty()) {
      return;
    }
    if (dynamicStore.supportAllowNewResourceModel()) {
      if (accountCapsule.oldBitPowerIsInvalid()) {
        switch (unfreezeBalanceV2Contract.getResource()) {
          case BANDWIDTH:
          case ENERGY:
            // there is no need to change votes
            return;
          default:
            break;
        }
      } else {
        // clear all votes at once when new resource model start
        VotesCapsule votesCapsule;
        if (!votesStore.has(ownerAddress)) {
          votesCapsule = new VotesCapsule(
              unfreezeBalanceV2Contract.getOwnerAddress(),
              accountCapsule.getVotesList()
          );
        } else {
          votesCapsule = votesStore.get(ownerAddress);
        }
        accountCapsule.clearVotes();
        votesCapsule.clearNewVotes();
        votesStore.put(ownerAddress, votesCapsule);
        return;
      }
    }

    long totalVote = 0;
    for (Protocol.Vote vote : accountCapsule.getVotesList()) {
      totalVote += vote.getVoteCount();
    }
    BigInteger ownedBitPower;
    if (dynamicStore.supportAllowNewResourceModel()) {
      ownedBitPower = accountCapsule.getAllBitPower();
    } else {
      ownedBitPower = accountCapsule.getBitPower();
    }

    // bit power is enough to total votes
    if (ownedBitPower.compareTo(BigIntegerUtil.newInstance(String.valueOf(totalVote)).multiply(BigInteger.valueOf(BIT_PRECISION))) >= 0) {
      return;
    }
    if (totalVote == 0) {
      return;
    }

    VotesCapsule votesCapsule;
    if (!votesStore.has(ownerAddress)) {
      votesCapsule = new VotesCapsule(
          unfreezeBalanceV2Contract.getOwnerAddress(),
          accountCapsule.getVotesList()
      );
    } else {
      votesCapsule = votesStore.get(ownerAddress);
    }

    // Update Owner Voting
    List<Vote> addVotes = new ArrayList<>();
    for (Vote vote : accountCapsule.getVotesList()) {
      long newVoteCount = (long)
          ((double) vote.getVoteCount() / totalVote * ownedBitPower.divide(BigInteger.valueOf(BIT_PRECISION)).doubleValue());
      if (newVoteCount > 0) {
        Vote newVote = Vote.newBuilder()
            .setVoteAddress(vote.getVoteAddress())
            .setVoteCount(newVoteCount)
            .build();
        addVotes.add(newVote);
      }
    }
    votesCapsule.clearNewVotes();
    votesCapsule.addAllNewVotes(addVotes);
    votesStore.put(ownerAddress, votesCapsule);

    accountCapsule.clearVotes();
    accountCapsule.addAllVotes(addVotes);
  }
}