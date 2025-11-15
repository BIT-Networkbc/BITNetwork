package org.bit.core.actuator;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Arrays;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.bit.common.utils.DecodeUtil;
import org.bit.common.utils.StringUtil;
import org.bit.core.capsule.AccountCapsule;
import org.bit.core.capsule.ContractCapsule;
import org.bit.core.capsule.ReceiptCapsule;
import org.bit.core.capsule.TransactionResultCapsule;
import org.bit.core.exception.ContractExeException;
import org.bit.core.exception.ContractValidateException;
import org.bit.core.store.AccountStore;
import org.bit.core.store.ContractStore;
import org.bit.core.vm.repository.RepositoryImpl;
import org.bit.protos.Protocol.Transaction.Contract.ContractType;
import org.bit.protos.Protocol.Transaction.Result.code;
import org.bit.protos.contract.SmartContractOuterClass.UpdateEnergyLimitContract;

@Slf4j(topic = "actuator")
public class UpdateEnergyLimitContractActuator extends AbstractActuator {

  public UpdateEnergyLimitContractActuator() {
    super(ContractType.UpdateEnergyLimitContract, UpdateEnergyLimitContract.class);
  }

  @Override
  public boolean execute(Object object) throws ContractExeException {
    TransactionResultCapsule ret = (TransactionResultCapsule) object;
    if (Objects.isNull(ret)) {
      throw new RuntimeException(ActuatorConstant.TX_RESULT_NULL);
    }

    long fee = calcFee();
    ContractStore contractStore = chainBaseManager.getContractStore();
    try {
      UpdateEnergyLimitContract usContract = any.unpack(UpdateEnergyLimitContract.class);
      long newOriginEnergyLimit = usContract.getOriginEnergyLimit();
      byte[] contractAddress = usContract.getContractAddress().toByteArray();
      ContractCapsule deployedContract = contractStore.get(contractAddress);

      contractStore.put(contractAddress, new ContractCapsule(
          deployedContract.getInstance().toBuilder().setOriginEnergyLimit(newOriginEnergyLimit)
              .build()));
      RepositoryImpl.removeLruCache(contractAddress);

      ret.setStatus(fee, code.SUCESS);
    } catch (InvalidProtocolBufferException e) {
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
    if (!ReceiptCapsule.checkForEnergyLimit(chainBaseManager.getDynamicPropertiesStore())) {
      throw new ContractValidateException(
          "contract type error, unexpected type [UpdateEnergyLimitContract]");
    }
    AccountStore accountStore = chainBaseManager.getAccountStore();
    ContractStore contractStore = chainBaseManager.getContractStore();
    if (!this.any.is(UpdateEnergyLimitContract.class)) {
      throw new ContractValidateException(
          "contract type error, expected type [UpdateEnergyLimitContract],real type["
              + any.getClass() + "]");
    }
    final UpdateEnergyLimitContract contract;
    try {
      contract = this.any.unpack(UpdateEnergyLimitContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }
    if (!DecodeUtil.addressValid(contract.getOwnerAddress().toByteArray())) {
      throw new ContractValidateException("Invalid address");
    }
    byte[] ownerAddress = contract.getOwnerAddress().toByteArray();
    String readableOwnerAddress = StringUtil.createReadableString(ownerAddress);

    AccountCapsule accountCapsule = accountStore.get(ownerAddress);
    if (accountCapsule == null) {
      throw new ContractValidateException(
          ActuatorConstant.ACCOUNT_EXCEPTION_STR + readableOwnerAddress + "] does not exist");
    }

    long newOriginEnergyLimit = contract.getOriginEnergyLimit();
    if (newOriginEnergyLimit <= 0) {
      throw new ContractValidateException(
          "origin energy limit must be > 0");
    }

    byte[] contractAddress = contract.getContractAddress().toByteArray();
    ContractCapsule deployedContract = contractStore.get(contractAddress);

    if (deployedContract == null) {
      throw new ContractValidateException(
          "Contract does not exist");
    }

    byte[] deployedContractOwnerAddress = deployedContract.getInstance().getOriginAddress()
        .toByteArray();

    if (!Arrays.equals(ownerAddress, deployedContractOwnerAddress)) {
      throw new ContractValidateException(
          ActuatorConstant.ACCOUNT_EXCEPTION_STR
              + readableOwnerAddress + "] is not the owner of the contract");
    }

    return true;
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return any.unpack(UpdateEnergyLimitContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return 0;
  }

}
