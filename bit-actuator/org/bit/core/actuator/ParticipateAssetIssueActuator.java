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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.bit.common.utils.BigIntegerUtil;
import org.bit.common.utils.ByteArray;
import org.bit.common.utils.Commons;
import org.bit.common.utils.DecodeUtil;
import org.bit.core.capsule.AccountCapsule;
import org.bit.core.capsule.AssetIssueCapsule;
import org.bit.core.capsule.TransactionResultCapsule;
import org.bit.core.exception.ContractExeException;
import org.bit.core.exception.ContractValidateException;
import org.bit.core.store.AccountStore;
import org.bit.core.store.AssetIssueStore;
import org.bit.core.store.AssetIssueV2Store;
import org.bit.core.store.DynamicPropertiesStore;
import org.bit.protos.Protocol;
import org.bit.protos.Protocol.Transaction.Contract.ContractType;
import org.bit.protos.Protocol.Transaction.Result.code;
import org.bit.protos.contract.AssetIssueContractOuterClass.ParticipateAssetIssueContract;

@Slf4j(topic = "actuator")
public class ParticipateAssetIssueActuator extends AbstractActuator {

  public ParticipateAssetIssueActuator() {
    super(ContractType.ParticipateAssetIssueContract, ParticipateAssetIssueContract.class);
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
    AssetIssueStore assetIssueStore = chainBaseManager.getAssetIssueStore();
    AssetIssueV2Store assetIssueV2Store = chainBaseManager.getAssetIssueV2Store();
    try {
      final ParticipateAssetIssueContract participateAssetIssueContract =
          any.unpack(ParticipateAssetIssueContract.class);
      BigInteger cost = BigIntegerUtil.newInstance(participateAssetIssueContract.getAmount());

      //subtract from owner address
      byte[] ownerAddress = participateAssetIssueContract.getOwnerAddress().toByteArray();
      AccountCapsule ownerAccount = accountStore.get(ownerAddress);
      BigInteger balance = ownerAccount.getBalance().subtract(cost);
      balance = balance.subtract(BigInteger.valueOf(fee));
      ownerAccount.setBalance(balance.toString());
      byte[] key = participateAssetIssueContract.getAssetName().toByteArray();

      //calculate the exchange amount
      AssetIssueCapsule assetIssueCapsule;
      assetIssueCapsule = Commons
          .getAssetIssueStoreFinal(dynamicStore, assetIssueStore, assetIssueV2Store).get(key);

      BigInteger exchangeAmount = cost.multiply(BigInteger.valueOf(assetIssueCapsule.getNum()));
      exchangeAmount = exchangeAmount.divide(BigIntegerUtil.newInstance(String.valueOf(assetIssueCapsule.getBitNum())));
      ownerAccount.addAssetAmountV2(key, exchangeAmount, dynamicStore, assetIssueStore);

      //add to to_address
      byte[] toAddress = participateAssetIssueContract.getToAddress().toByteArray();
      AccountCapsule toAccount = accountStore.get(toAddress);
      toAccount.setBalance(toAccount.getBalance().add(cost).toString());
      if (!toAccount.reduceAssetAmountV2(key, exchangeAmount, dynamicStore, assetIssueStore)) {
        throw new ContractExeException("reduceAssetAmount failed !");
      }

      //write to db
      accountStore.put(ownerAddress, ownerAccount);
      accountStore.put(toAddress, toAccount);
      ret.setStatus(fee, Protocol.Transaction.Result.code.SUCESS);
    } catch (InvalidProtocolBufferException | ArithmeticException e) {
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
    AssetIssueStore assetIssueStore = chainBaseManager.getAssetIssueStore();
    AssetIssueV2Store assetIssueV2Store = chainBaseManager.getAssetIssueV2Store();
    if (!this.any.is(ParticipateAssetIssueContract.class)) {
      throw new ContractValidateException(
          "contract type error,expected type [ParticipateAssetIssueContract],real type[" + any
              .getClass() + "]");
    }

    final ParticipateAssetIssueContract participateAssetIssueContract;
    try {
      participateAssetIssueContract =
          this.any.unpack(ParticipateAssetIssueContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }
    //Parameters check
    byte[] ownerAddress = participateAssetIssueContract.getOwnerAddress().toByteArray();
    byte[] toAddress = participateAssetIssueContract.getToAddress().toByteArray();
    byte[] assetName = participateAssetIssueContract.getAssetName().toByteArray();
    BigInteger amount = BigIntegerUtil.newInstance(participateAssetIssueContract.getAmount());

    if (!DecodeUtil.addressValid(ownerAddress)) {
      throw new ContractValidateException("Invalid ownerAddress");
    }
    if (!DecodeUtil.addressValid(toAddress)) {
      throw new ContractValidateException("Invalid toAddress");
    }
//    if (!TransactionUtil.validAssetName(assetName)) {
//      throw new ContractValidateException("Invalid assetName");
//    }
    if (amount.compareTo(BigInteger.ZERO) <= 0) {
      throw new ContractValidateException("Amount must greater than 0!");
    }

    if (Arrays.equals(ownerAddress, toAddress)) {
      throw new ContractValidateException("Cannot participate asset Issue yourself !");
    }

    //Whether the accountStore exist
    AccountCapsule ownerAccount = accountStore.get(ownerAddress);
    if (ownerAccount == null) {
      throw new ContractValidateException("Account does not exist!");
    }
    try {
      //Whether the balance is enough
      long fee = calcFee();
      if (ownerAccount.getBalance().compareTo(amount.add(BigInteger.valueOf(fee))) < 0) {
        throw new ContractValidateException("No enough balance !");
      }

      //Whether have the mapping
      AssetIssueCapsule assetIssueCapsule;
      assetIssueCapsule = Commons
          .getAssetIssueStoreFinal(dynamicStore, assetIssueStore, assetIssueV2Store).get(assetName);
      if (assetIssueCapsule == null) {
        throw new ContractValidateException("No asset named " + ByteArray.toStr(assetName));
      }

      if (!Arrays.equals(toAddress, assetIssueCapsule.getOwnerAddress().toByteArray())) {
        throw new ContractValidateException(
            "The asset is not issued by " + ByteArray.toHexString(toAddress));
      }
      //Whether the exchange can be processed: to see if the exchange can be the exact int
      long now = dynamicStore.getLatestBlockHeaderTimestamp();
      if (now >= assetIssueCapsule.getEndTime() || now < assetIssueCapsule
          .getStartTime()) {
        throw new ContractValidateException("No longer valid period!");
      }

      int bitNum = assetIssueCapsule.getBitNum();
      int num = assetIssueCapsule.getNum();
      BigInteger exchangeAmount = amount.multiply(BigInteger.valueOf(num));
      exchangeAmount = exchangeAmount.divide(BigInteger.valueOf(bitNum));
      if (exchangeAmount.compareTo(BigInteger.ZERO) <= 0) {
        throw new ContractValidateException("Can not process the exchange!");
      }

      AccountCapsule toAccount = accountStore.get(toAddress);
      if (toAccount == null) {
        throw new ContractValidateException("To account does not exist!");
      }

      if (!toAccount.assetBalanceEnoughV2(assetName, exchangeAmount,
          dynamicStore)) {
        throw new ContractValidateException("Asset balance is not enough !");
      }
    } catch (ArithmeticException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }

    return true;
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return this.any.unpack(ParticipateAssetIssueContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return 0;
  }
}
