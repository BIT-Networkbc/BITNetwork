/*
 * java-bit is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * java-bit is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.bit.core.actuator;

import static org.bit.core.config.Parameter.ChainConstant.FROZEN_PERIOD;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.bit.common.utils.BigIntegerUtil;
import org.bit.common.utils.Commons;
import org.bit.common.utils.DecodeUtil;
import org.bit.core.capsule.AccountCapsule;
import org.bit.core.capsule.AssetIssueCapsule;
import org.bit.core.capsule.TransactionResultCapsule;
import org.bit.core.exception.BalanceInsufficientException;
import org.bit.core.exception.ContractExeException;
import org.bit.core.exception.ContractValidateException;
import org.bit.core.store.AccountStore;
import org.bit.core.store.AssetIssueStore;
import org.bit.core.store.AssetIssueV2Store;
import org.bit.core.store.DynamicPropertiesStore;
import org.bit.core.utils.TransactionUtil;
import org.bit.protos.Protocol.Account.Frozen;
import org.bit.protos.Protocol.Transaction.Contract.ContractType;
import org.bit.protos.Protocol.Transaction.Result.code;
import org.bit.protos.contract.AssetIssueContractOuterClass.AssetIssueContract;
import org.bit.protos.contract.AssetIssueContractOuterClass.AssetIssueContract.FrozenSupply;

@Slf4j(topic = "actuator")
public class AssetIssueActuator extends AbstractActuator {

  public AssetIssueActuator() {
    super(ContractType.AssetIssueContract, AssetIssueContract.class);
  }

  @Override
  public boolean execute(Object result) throws ContractExeException {
    TransactionResultCapsule ret = (TransactionResultCapsule) result;
    if (Objects.isNull(ret)) {
      throw new RuntimeException(ActuatorConstant.TX_RESULT_NULL);
    }

    long fee = calcFee();
    DynamicPropertiesStore dynamicStore = chainBaseManager.getDynamicPropertiesStore();
    AssetIssueStore assetIssueStore = chainBaseManager.getAssetIssueStore();
    AssetIssueV2Store assetIssueV2Store = chainBaseManager.getAssetIssueV2Store();
    AccountStore accountStore = chainBaseManager.getAccountStore();
    try {
      AssetIssueContract assetIssueContract = any.unpack(AssetIssueContract.class);
      byte[] ownerAddress = assetIssueContract.getOwnerAddress().toByteArray();
      AssetIssueCapsule assetIssueCapsule = new AssetIssueCapsule(assetIssueContract);
      AssetIssueCapsule assetIssueCapsuleV2 = new AssetIssueCapsule(assetIssueContract);
      long tokenIdNum = dynamicStore.getTokenIdNum();
      tokenIdNum++;
      assetIssueCapsule.setId(Long.toString(tokenIdNum));
      assetIssueCapsuleV2.setId(Long.toString(tokenIdNum));
      dynamicStore.saveTokenIdNum(tokenIdNum);

      if (dynamicStore.getAllowSameTokenName() == 0) {
        assetIssueCapsuleV2.setPrecision(0);
        assetIssueStore
            .put(assetIssueCapsule.createDbKey(), assetIssueCapsule);
        assetIssueV2Store
            .put(assetIssueCapsuleV2.createDbV2Key(), assetIssueCapsuleV2);
      } else {
        assetIssueV2Store
            .put(assetIssueCapsuleV2.createDbV2Key(), assetIssueCapsuleV2);
      }

      Commons.adjustBalance(accountStore, ownerAddress, BigInteger.valueOf(-fee));
      // UPD create witness fee to feeManager
//      if (dynamicStore.supportBlackHoleOptimization()) {
//        dynamicStore.burnBit(fee);
//      } else {
//        Commons.adjustBalance(accountStore, accountStore.getBlackhole(), BigInteger.valueOf(fee));//send to blackhole
//      }
      Commons.adjustBalance(accountStore, accountStore.getGasFeeManagerAddress(), BigInteger.valueOf(+fee));

      AccountCapsule accountCapsule = accountStore.get(ownerAddress);
      List<FrozenSupply> frozenSupplyList = assetIssueContract.getFrozenSupplyList();
      Iterator<FrozenSupply> iterator = frozenSupplyList.iterator();
      BigInteger remainSupply = BigIntegerUtil.newInstance(assetIssueContract.getTotalSupply());
      List<Frozen> frozenList = new ArrayList<>();
      long startTime = assetIssueContract.getStartTime();

      while (iterator.hasNext()) {
        FrozenSupply next = iterator.next();
        long expireTime = startTime + next.getFrozenDays() * FROZEN_PERIOD;
        Frozen newFrozen = Frozen.newBuilder()
            .setFrozenBalance(next.getFrozenAmount())
            .setExpireTime(expireTime)
            .build();
        frozenList.add(newFrozen);
        remainSupply = remainSupply.subtract(BigIntegerUtil.newInstance(next.getFrozenAmount()));
      }

      if (dynamicStore.getAllowSameTokenName() == 0) {
        accountCapsule.addAsset(assetIssueCapsule.createDbKey(), String.valueOf(remainSupply));
      }
      accountCapsule.setAssetIssuedName(assetIssueCapsule.createDbKey());
      accountCapsule.setAssetIssuedID(assetIssueCapsule.createDbV2Key());
      accountCapsule.addAssetV2(assetIssueCapsuleV2.createDbV2Key(), String.valueOf(remainSupply));
      accountCapsule.setInstance(accountCapsule.getInstance().toBuilder()
          .addAllFrozenSupply(frozenList).build());

      accountStore.put(ownerAddress, accountCapsule);

      ret.setAssetIssueID(Long.toString(tokenIdNum));
      ret.setStatus(fee, code.SUCESS);
    } catch (InvalidProtocolBufferException | BalanceInsufficientException | ArithmeticException e) {
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
    DynamicPropertiesStore dynamicStore = chainBaseManager.getDynamicPropertiesStore();
    AssetIssueStore assetIssueStore = chainBaseManager.getAssetIssueStore();
    AccountStore accountStore = chainBaseManager.getAccountStore();
    if (!this.any.is(AssetIssueContract.class)) {
      throw new ContractValidateException(
          "contract type error,expected type [AssetIssueContract],real type[" + any
              .getClass() + "]");
    }

    final AssetIssueContract assetIssueContract;
    try {
      assetIssueContract = this.any.unpack(AssetIssueContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }

    byte[] ownerAddress = assetIssueContract.getOwnerAddress().toByteArray();
    if (!DecodeUtil.addressValid(ownerAddress)) {
      throw new ContractValidateException("Invalid ownerAddress");
    }

    if (!TransactionUtil.validAssetName(assetIssueContract.getName().toByteArray())) {
      throw new ContractValidateException("Invalid assetName");
    }

    if (dynamicStore.getAllowSameTokenName() != 0) {
      String name = assetIssueContract.getName().toStringUtf8().toLowerCase();
      if (("bit").equals(name)) {
        throw new ContractValidateException("assetName can't be bit");
      }
    }

    int precision = assetIssueContract.getPrecision();
    if (precision != 0
        && dynamicStore.getAllowSameTokenName() != 0
        && (precision < 0 || precision > ActuatorConstant.PRECISION_DECIMAL)) {
      throw new ContractValidateException("precision cannot exceed 6");
    }

    if ((!assetIssueContract.getAbbr().isEmpty()) && !TransactionUtil
        .validAssetName(assetIssueContract.getAbbr().toByteArray())) {
      throw new ContractValidateException("Invalid abbreviation for token");
    }

    if (!TransactionUtil.validUrl(assetIssueContract.getUrl().toByteArray())) {
      throw new ContractValidateException("Invalid url");
    }

    if (!TransactionUtil
        .validAssetDescription(assetIssueContract.getDescription().toByteArray())) {
      throw new ContractValidateException("Invalid description");
    }

    if (assetIssueContract.getStartTime() == 0) {
      throw new ContractValidateException("Start time should be not empty");
    }
    if (assetIssueContract.getEndTime() == 0) {
      throw new ContractValidateException("End time should be not empty");
    }
    if (assetIssueContract.getEndTime() <= assetIssueContract.getStartTime()) {
      throw new ContractValidateException("End time should be greater than start time");
    }
    if (assetIssueContract.getStartTime() <= dynamicStore.getLatestBlockHeaderTimestamp()) {
      throw new ContractValidateException("Start time should be greater than HeadBlockTime");
    }

    if (dynamicStore.getAllowSameTokenName() == 0
        && assetIssueStore.get(assetIssueContract.getName().toByteArray())
        != null) {
      throw new ContractValidateException("Token exists");
    }

    if (BigIntegerUtil.newInstance(assetIssueContract.getTotalSupply()).compareTo(BigInteger.ZERO) <= 0) {
      throw new ContractValidateException("TotalSupply must greater than 0!");
    }

    if (assetIssueContract.getBitNum() <= 0) {
      throw new ContractValidateException("BitNum must greater than 0!");
    }

    if (assetIssueContract.getNum() <= 0) {
      throw new ContractValidateException("Num must greater than 0!");
    }

    if (assetIssueContract.getPublicFreeAssetNetUsage() != 0) {
      throw new ContractValidateException("PublicFreeAssetNetUsage must be 0!");
    }

    if (assetIssueContract.getFrozenSupplyCount()
        > dynamicStore.getMaxFrozenSupplyNumber()) {
      throw new ContractValidateException("Frozen supply list length is too long");
    }

    if (assetIssueContract.getFreeAssetNetLimit() < 0
        || assetIssueContract.getFreeAssetNetLimit() >=
        dynamicStore.getOneDayNetLimit()) {
      throw new ContractValidateException("Invalid FreeAssetNetLimit");
    }

    if (assetIssueContract.getPublicFreeAssetNetLimit() < 0
        || assetIssueContract.getPublicFreeAssetNetLimit() >=
        dynamicStore.getOneDayNetLimit()) {
      throw new ContractValidateException("Invalid PublicFreeAssetNetLimit");
    }

    BigInteger remainSupply = BigIntegerUtil.newInstance(assetIssueContract.getTotalSupply());
    long minFrozenSupplyTime = dynamicStore.getMinFrozenSupplyTime();
    long maxFrozenSupplyTime = dynamicStore.getMaxFrozenSupplyTime();
    List<FrozenSupply> frozenList = assetIssueContract.getFrozenSupplyList();
    Iterator<FrozenSupply> iterator = frozenList.iterator();

    while (iterator.hasNext()) {
      FrozenSupply next = iterator.next();
      if (BigIntegerUtil.newInstance(next.getFrozenAmount()).compareTo(BigInteger.ZERO) <= 0) {
        throw new ContractValidateException("Frozen supply must be greater than 0!");
      }
      if (BigIntegerUtil.newInstance(next.getFrozenAmount()).compareTo(remainSupply) > 0) {
        throw new ContractValidateException("Frozen supply cannot exceed total supply");
      }
      if (!(next.getFrozenDays() >= minFrozenSupplyTime
          && next.getFrozenDays() <= maxFrozenSupplyTime)) {
        throw new ContractValidateException(
            "frozenDuration must be less than " + maxFrozenSupplyTime + " days "
                + "and more than " + minFrozenSupplyTime + " days");
      }
      remainSupply = remainSupply.subtract(BigIntegerUtil.newInstance(next.getFrozenAmount()));
    }

    AccountCapsule accountCapsule = accountStore.get(ownerAddress);
    if (accountCapsule == null) {
      throw new ContractValidateException("Account not exists");
    }

    if (!accountCapsule.getAssetIssuedName().isEmpty()) {
      throw new ContractValidateException("An account can only issue one asset");
    }

    if (accountCapsule.getBalance().compareTo(BigInteger.valueOf(calcFee())) < 0) {
      throw new ContractValidateException("No enough balance for fee!");
    }
    return true;
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return any.unpack(AssetIssueContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return chainBaseManager.getDynamicPropertiesStore().getAssetIssueFee();
  }

}
