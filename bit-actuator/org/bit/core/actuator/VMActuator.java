package org.bit.core.actuator;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.commons.lang3.ArrayUtils.getLength;
import static org.apache.commons.lang3.ArrayUtils.isNotEmpty;
import static org.bit.protos.contract.Common.ResourceCode.ENERGY;

import com.google.protobuf.ByteString;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.util.encoders.Hex;
import org.bit.common.logsfilter.trigger.ContractTrigger;
import org.bit.common.parameter.CommonParameter;
import org.bit.common.runtime.InternalTransaction;
import org.bit.common.runtime.InternalTransaction.ExecutorType;
import org.bit.common.runtime.InternalTransaction.BitType;
import org.bit.common.runtime.ProgramResult;
import org.bit.common.runtime.vm.DataWord;
import org.bit.common.utils.BigIntegerUtil;
import org.bit.common.utils.StorageUtils;
import org.bit.common.utils.StringUtil;
import org.bit.common.utils.WalletUtil;
import org.bit.core.ChainBaseManager;
import org.bit.core.capsule.AccountCapsule;
import org.bit.core.capsule.BlockCapsule;
import org.bit.core.capsule.ContractCapsule;
import org.bit.core.capsule.ReceiptCapsule;
import org.bit.core.db.EnergyProcessor;
import org.bit.core.db.TransactionContext;
import org.bit.core.exception.ContractExeException;
import org.bit.core.exception.ContractValidateException;
import org.bit.core.utils.TransactionUtil;
import org.bit.core.vm.EnergyCost;
import org.bit.core.vm.LogInfoTriggerParser;
import org.bit.core.vm.OperationRegistry;
import org.bit.core.vm.VM;
import org.bit.core.vm.VMConstant;
import org.bit.core.vm.VMUtils;
import org.bit.core.vm.config.ConfigLoader;
import org.bit.core.vm.config.VMConfig;
import org.bit.core.vm.program.Program;
import org.bit.core.vm.program.Program.JVMStackOverFlowException;
import org.bit.core.vm.program.Program.OutOfTimeException;
import org.bit.core.vm.program.Program.TransferException;
import org.bit.core.vm.program.ProgramPrecompile;
import org.bit.core.vm.program.invoke.ProgramInvoke;
import org.bit.core.vm.program.invoke.ProgramInvokeFactory;
import org.bit.core.vm.repository.Repository;
import org.bit.core.vm.repository.RepositoryImpl;
import org.bit.core.vm.utils.MUtil;
import org.bit.protos.Protocol;
import org.bit.protos.Protocol.Block;
import org.bit.protos.Protocol.Transaction;
import org.bit.protos.Protocol.Transaction.Contract.ContractType;
import org.bit.protos.Protocol.Transaction.Result.contractResult;
import org.bit.protos.contract.SmartContractOuterClass.CreateSmartContract;
import org.bit.protos.contract.SmartContractOuterClass.SmartContract;
import org.bit.protos.contract.SmartContractOuterClass.TriggerSmartContract;

@Slf4j(topic = "VM")
public class VMActuator implements Actuator2 {

  /* tx and block info */
  private Transaction bit;
  private BlockCapsule blockCap;

  /* tvm execution context */
  private Repository rootRepository;
  private Program program;
  private InternalTransaction rootInternalTx;

  /* tx receipt */
  private ReceiptCapsule receipt;

  @Getter
  @Setter
  private InternalTransaction.BitType bitType;
  private ExecutorType executorType;

  @Getter
  @Setter
  private boolean isConstantCall;

  private long maxEnergyLimit;

  @Setter
  private boolean enableEventListener;

  private LogInfoTriggerParser logInfoTriggerParser;

  public VMActuator(boolean isConstantCall) {
    this.isConstantCall = isConstantCall;
    this.maxEnergyLimit = CommonParameter.getInstance().maxEnergyLimitForConstant;
  }

  private static long getEnergyFee(BigInteger callerEnergyUsage, BigInteger callerEnergyFrozen,
                                   BigInteger callerEnergyTotal) {
    if (callerEnergyTotal.compareTo(BigInteger.ZERO) <= 0) {
      return 0;
    }
    return callerEnergyFrozen.multiply(callerEnergyUsage)
        .divide(callerEnergyTotal).longValueExact();
  }

  @Override
  public void validate(Object object) throws ContractValidateException {

    TransactionContext context = (TransactionContext) object;
    if (Objects.isNull(context)) {
      throw new RuntimeException("TransactionContext is null");
    }

    // Load Config
    ConfigLoader.load(context.getStoreFactory());
    // Warm up registry class
    OperationRegistry.init();
    bit = context.getBitCap().getInstance();
    // If tx`s fee limit is set, use it to calc max energy limit for constant call
    if (isConstantCall && bit.getRawData().getFeeLimit() > 0) {
      maxEnergyLimit = Math.min(maxEnergyLimit, bit.getRawData().getFeeLimit()
          / context.getStoreFactory().getChainBaseManager()
          .getDynamicPropertiesStore().getEnergyFee());
    }
    blockCap = context.getBlockCap();
    if ((VMConfig.allowTvmFreeze() || VMConfig.allowTvmFreezeV2())
        && context.getBitCap().getBitTrace() != null) {
      receipt = context.getBitCap().getBitTrace().getReceipt();
    }
    //Route Type
    ContractType contractType = this.bit.getRawData().getContract(0).getType();
    //Prepare Repository
    rootRepository = RepositoryImpl.createRoot(context.getStoreFactory());

    enableEventListener = context.isEventPluginLoaded();

    //set executorType type
    if (Objects.nonNull(blockCap)) {
      this.executorType = ExecutorType.ET_NORMAL_TYPE;
    } else {
      this.blockCap = new BlockCapsule(Block.newBuilder().build());
      this.executorType = ExecutorType.ET_PRE_TYPE;
    }
    if (isConstantCall) {
      this.executorType = ExecutorType.ET_PRE_TYPE;
    }

    switch (contractType.getNumber()) {
      case ContractType.TriggerSmartContract_VALUE:
        bitType = BitType.BIT_CONTRACT_CALL_TYPE;
        call();
        break;
      case ContractType.CreateSmartContract_VALUE:
        bitType = BitType.BIT_CONTRACT_CREATION_TYPE;
        create();
        break;
      default:
        throw new ContractValidateException("Unknown contract type");
    }
  }

  @Override
  public void execute(Object object) throws ContractExeException {
    TransactionContext context = (TransactionContext) object;
    if (Objects.isNull(context)) {
      throw new RuntimeException("TransactionContext is null");
    }

    ProgramResult result = context.getProgramResult();
    try {
      if (program != null) {
        if (null != blockCap && blockCap.generatedByMyself && blockCap.hasWitnessSignature()
            && null != TransactionUtil.getContractRet(bit)
            && contractResult.OUT_OF_TIME == TransactionUtil.getContractRet(bit)) {
          result = program.getResult();
          program.spendAllEnergy();

          OutOfTimeException e = Program.Exception.alreadyTimeOut();
          result.setRuntimeError(e.getMessage());
          result.setException(e);
          throw e;
        }

        VM.play(program, OperationRegistry.getTable());
        result = program.getResult();

        if (BitType.BIT_CONTRACT_CREATION_TYPE == bitType && !result.isRevert()) {
          byte[] code = program.getResult().getHReturn();
          if (code.length != 0 && VMConfig.allowTvmLondon() && code[0] == (byte) 0xEF) {
            if (null == result.getException()) {
              result.setException(Program.Exception.invalidCodeException());
            }
          }
          long saveCodeEnergy = (long) getLength(code) * EnergyCost.getCreateData();
          long afterSpend = program.getEnergyLimitLeft().longValue() - saveCodeEnergy;
          if (afterSpend < 0) {
            if (null == result.getException()) {
              result.setException(Program.Exception
                  .notEnoughSpendEnergy("save just created contract code",
                      saveCodeEnergy, program.getEnergyLimitLeft().longValue()));
            }
          } else {
            result.spendEnergy(saveCodeEnergy);
            if (VMConfig.allowTvmConstantinople()) {
              rootRepository.saveCode(program.getContractAddress().getNoLeadZeroesData(), code);
            }
          }
        }

        if (isConstantCall) {
          if (result.getException() != null) {
            result.setRuntimeError(result.getException().getMessage());
            result.rejectInternalTransactions();
          }
          context.setProgramResult(result);
          return;
        }

        if (result.getException() != null || result.isRevert()) {
          result.getDeleteAccounts().clear();
          result.getLogInfoList().clear();
          //result.resetFutureRefund();
          result.rejectInternalTransactions();

          if (result.getException() != null) {
            if (!(result.getException() instanceof TransferException)) {
              program.spendAllEnergy();
            }
            result.setRuntimeError(result.getException().getMessage());
            throw result.getException();
          } else {
            result.setRuntimeError("REVERT opcode executed");
          }
        } else {
          rootRepository.commit();

          if (logInfoTriggerParser != null) {
            List<ContractTrigger> triggers = logInfoTriggerParser
                .parseLogInfos(program.getResult().getLogInfoList(), rootRepository);
            program.getResult().setTriggerList(triggers);
          }

        }
      } else {
        rootRepository.commit();
      }
      for (DataWord account : result.getDeleteAccounts()) {
        RepositoryImpl.removeLruCache(account.toBitAddress());
      }
    } catch (JVMStackOverFlowException e) {
      program.spendAllEnergy();
      result = program.getResult();
      result.setException(e);
      result.rejectInternalTransactions();
      result.setRuntimeError(result.getException().getMessage());
      logger.info("JVMStackOverFlowException: {}", result.getException().getMessage());
    } catch (OutOfTimeException e) {
      program.spendAllEnergy();
      result = program.getResult();
      result.setException(e);
      result.rejectInternalTransactions();
      result.setRuntimeError(result.getException().getMessage());
      logger.info("timeout: {}", result.getException().getMessage());
    } catch (Throwable e) {
      if (!(e instanceof TransferException)) {
        program.spendAllEnergy();
      }
      result = program.getResult();
      result.rejectInternalTransactions();
      if (Objects.isNull(result.getException())) {
        logger.error(e.getMessage(), e);
        result.setException(new RuntimeException("Unknown Throwable"));
      }
      if (StringUtils.isEmpty(result.getRuntimeError())) {
        result.setRuntimeError(result.getException().getMessage());
      }
      logger.info("runtime result is :{}", result.getException().getMessage());
    }
    //use program returned fill context
    context.setProgramResult(result);

    if (VMConfig.vmTrace() && program != null) {
      String traceContent = program.getTrace()
          .result(result.getHReturn())
          .error(result.getException())
          .toString();

      if (VMConfig.vmTraceCompressed()) {
        traceContent = VMUtils.zipAndEncode(traceContent);
      }

      String txHash = Hex.toHexString(rootInternalTx.getHash());
      VMUtils.saveProgramTraceFile(txHash, traceContent);
    }

  }

  private void create()
      throws ContractValidateException {
    if (!rootRepository.getDynamicPropertiesStore().supportVM()) {
      throw new ContractValidateException("vm work is off, need to be opened by the committee");
    }

    CreateSmartContract contract = ContractCapsule.getSmartContractFromTransaction(bit);
    if (contract == null) {
      throw new ContractValidateException("Cannot get CreateSmartContract from transaction");
    }
    SmartContract newSmartContract;
    if (VMConfig.allowTvmCompatibleEvm()) {
      newSmartContract = contract.getNewContract().toBuilder().setVersion(1).build();
    } else {
      newSmartContract = contract.getNewContract().toBuilder().clearVersion().build();
    }
    if (!contract.getOwnerAddress().equals(newSmartContract.getOriginAddress())) {
      logger.info("OwnerAddress not equals OriginAddress");
      throw new ContractValidateException("OwnerAddress is not equals OriginAddress");
    }

    byte[] contractName = newSmartContract.getName().getBytes();

    if (contractName.length > VMConstant.CONTRACT_NAME_LENGTH) {
      throw new ContractValidateException("contractName's length cannot be greater than 32");
    }

    long percent = contract.getNewContract().getConsumeUserResourcePercent();
    if (percent < 0 || percent > VMConstant.ONE_HUNDRED) {
      throw new ContractValidateException("percent must be >= 0 and <= 100");
    }

    byte[] contractAddress = WalletUtil.generateContractAddress(bit);
    // insure the new contract address haven't exist
    if (rootRepository.getAccount(contractAddress) != null) {
      throw new ContractValidateException(
          "Trying to create a contract with existing contract address: " + StringUtil
              .encode58Check(contractAddress));
    }

    newSmartContract = newSmartContract.toBuilder()
        .setContractAddress(ByteString.copyFrom(contractAddress)).build();
    BigInteger callValue = BigIntegerUtil.newInstance(newSmartContract.getCallValue());
    BigInteger tokenValue = BigInteger.ZERO;
    long tokenId = 0;
    if (VMConfig.allowTvmTransferBrc10()) {
      tokenValue = BigIntegerUtil.newInstance(contract.getCallTokenValue());
      tokenId = contract.getTokenId();
    }
    byte[] callerAddress = contract.getOwnerAddress().toByteArray();
    // create vm to constructor smart contract
    try {
      long feeLimit = bit.getRawData().getFeeLimit();
      if (feeLimit < 0 || feeLimit > rootRepository.getDynamicPropertiesStore().getMaxFeeLimit()) {
        logger.info("invalid feeLimit {}", feeLimit);
        throw new ContractValidateException("feeLimit must be >= 0 and <= "
            + rootRepository.getDynamicPropertiesStore().getMaxFeeLimit());
      }
      AccountCapsule creator = rootRepository
          .getAccount(newSmartContract.getOriginAddress().toByteArray());

      long energyLimit;
      // according to version

      if (isConstantCall) {
        energyLimit = maxEnergyLimit;
      } else {
        if (StorageUtils.getEnergyLimitHardFork()) {
          if (callValue.compareTo(BigInteger.ZERO) < 0) {
            throw new ContractValidateException("callValue must be >= 0");
          }
          if (tokenValue.compareTo(BigInteger.ZERO) < 0) {
            throw new ContractValidateException("tokenValue must be >= 0");
          }
          if (newSmartContract.getOriginEnergyLimit() <= 0) {
            throw new ContractValidateException("The originEnergyLimit must be > 0");
          }
          energyLimit = getAccountEnergyLimitWithFixRatio(creator, BigInteger.valueOf(feeLimit), callValue).longValue();
        } else {
          energyLimit = getAccountEnergyLimitWithFloatRatio(creator, BigInteger.valueOf(feeLimit), callValue).longValue();
        }
      }

      checkTokenValueAndId(tokenValue, tokenId);

      byte[] ops = newSmartContract.getBytecode().toByteArray();
      rootInternalTx = new InternalTransaction(bit, bitType);

      long maxCpuTimeOfOneTx = rootRepository.getDynamicPropertiesStore()
          .getMaxCpuTimeOfOneTx() * VMConstant.ONE_THOUSAND;
      long thisTxCPULimitInUs = (long) (maxCpuTimeOfOneTx * getCpuLimitInUsRatio());
      long vmStartInUs = System.nanoTime() / VMConstant.ONE_THOUSAND;
      long vmShouldEndInUs = vmStartInUs + thisTxCPULimitInUs;
      ProgramInvoke programInvoke = ProgramInvokeFactory
          .createProgramInvoke(BitType.BIT_CONTRACT_CREATION_TYPE, executorType, bit,
              tokenValue, tokenId, blockCap.getInstance(), rootRepository, vmStartInUs,
              vmShouldEndInUs, energyLimit);
      if (isConstantCall) {
        programInvoke.setConstantCall();
      }
      this.program = new Program(ops, contractAddress, programInvoke, rootInternalTx);
      if (VMConfig.allowTvmCompatibleEvm()) {
        this.program.setContractVersion(1);
      }
      byte[] txId = TransactionUtil.getTransactionId(bit).getBytes();
      this.program.setRootTransactionId(txId);
      if (enableEventListener && isCheckTransaction()) {
        logInfoTriggerParser = new LogInfoTriggerParser(blockCap.getNum(), blockCap.getTimeStamp(),
            txId, callerAddress);
      }
    } catch (Exception e) {
      logger.info(e.getMessage());
      throw new ContractValidateException(e.getMessage());
    }
    program.getResult().setContractAddress(contractAddress);

    rootRepository.createAccount(contractAddress, newSmartContract.getName(),
        Protocol.AccountType.Contract);

    rootRepository.createContract(contractAddress, new ContractCapsule(newSmartContract));
    byte[] code = newSmartContract.getBytecode().toByteArray();
    if (!VMConfig.allowTvmConstantinople()) {
      rootRepository.saveCode(contractAddress, ProgramPrecompile.getCode(code));
    }
    // transfer from callerAddress to contractAddress according to callValue
    if (callValue.compareTo(BigInteger.ZERO) > 0) {
      MUtil.transfer(rootRepository, callerAddress, contractAddress, callValue);
    }
    if (VMConfig.allowTvmTransferBrc10() && tokenValue.compareTo(BigInteger.ZERO) > 0) {
      MUtil.transferToken(rootRepository, callerAddress, contractAddress, String.valueOf(tokenId),
          tokenValue);
    }

  }

  /**
   * **
   */

  private void call()
      throws ContractValidateException {

    if (!rootRepository.getDynamicPropertiesStore().supportVM()) {
      logger.info("vm work is off, need to be opened by the committee");
      throw new ContractValidateException("VM work is off, need to be opened by the committee");
    }

    TriggerSmartContract contract = ContractCapsule.getTriggerContractFromTransaction(bit);
    if (contract == null) {
      return;
    }

    if (contract.getContractAddress() == null) {
      throw new ContractValidateException("Cannot get contract address from TriggerContract");
    }

    byte[] contractAddress = contract.getContractAddress().toByteArray();

    ContractCapsule deployedContract = rootRepository.getContract(contractAddress);
    if (null == deployedContract) {
      logger.info("No contract or not a smart contract");
      throw new ContractValidateException("No contract or not a smart contract");
    }

    BigInteger callValue = BigIntegerUtil.newInstance(contract.getCallValue());
    BigInteger tokenValue = BigInteger.ZERO;
    long tokenId = 0;
    if (VMConfig.allowTvmTransferBrc10()) {
      tokenValue = BigIntegerUtil.newInstance(contract.getCallTokenValue());
      tokenId = contract.getTokenId();
    }

    if (StorageUtils.getEnergyLimitHardFork()) {
      if (callValue.compareTo(BigInteger.ZERO) < 0) {
        throw new ContractValidateException("callValue must be >= 0");
      }
      if (tokenValue.compareTo(BigInteger.ZERO) < 0) {
        throw new ContractValidateException("tokenValue must be >= 0");
      }
    }

    byte[] callerAddress = contract.getOwnerAddress().toByteArray();
    checkTokenValueAndId(tokenValue, tokenId);

    byte[] code = rootRepository.getCode(contractAddress);
    if (isNotEmpty(code)) {
      long feeLimit = bit.getRawData().getFeeLimit();
      if (feeLimit < 0 || feeLimit > rootRepository.getDynamicPropertiesStore().getMaxFeeLimit()) {
        logger.info("invalid feeLimit {}", feeLimit);
        throw new ContractValidateException("feeLimit must be >= 0 and <= "
            + rootRepository.getDynamicPropertiesStore().getMaxFeeLimit());
      }
      AccountCapsule caller = rootRepository.getAccount(callerAddress);
      long energyLimit;
      if (isConstantCall) {
        energyLimit = maxEnergyLimit;
      } else {
        AccountCapsule creator = rootRepository
            .getAccount(deployedContract.getInstance().getOriginAddress().toByteArray());
        energyLimit = getTotalEnergyLimit(creator, caller, contract, BigInteger.valueOf(feeLimit), callValue).longValue();
      }

      long maxCpuTimeOfOneTx = rootRepository.getDynamicPropertiesStore()
          .getMaxCpuTimeOfOneTx() * VMConstant.ONE_THOUSAND;
      long thisTxCPULimitInUs =
          (long) (maxCpuTimeOfOneTx * getCpuLimitInUsRatio());
      long vmStartInUs = System.nanoTime() / VMConstant.ONE_THOUSAND;
      long vmShouldEndInUs = vmStartInUs + thisTxCPULimitInUs;
      ProgramInvoke programInvoke = ProgramInvokeFactory
          .createProgramInvoke(BitType.BIT_CONTRACT_CALL_TYPE, executorType, bit,
              tokenValue, tokenId, blockCap.getInstance(), rootRepository, vmStartInUs,
              vmShouldEndInUs, energyLimit);
      if (isConstantCall) {
        programInvoke.setConstantCall();
      }
      rootInternalTx = new InternalTransaction(bit, bitType);
      this.program = new Program(code, contractAddress, programInvoke, rootInternalTx);
      if (VMConfig.allowTvmCompatibleEvm()) {
        this.program.setContractVersion(deployedContract.getContractVersion());
      }
      byte[] txId = TransactionUtil.getTransactionId(bit).getBytes();
      this.program.setRootTransactionId(txId);

      if (enableEventListener && isCheckTransaction()) {
        logInfoTriggerParser = new LogInfoTriggerParser(blockCap.getNum(), blockCap.getTimeStamp(),
            txId, callerAddress);
      }
    }

    program.getResult().setContractAddress(contractAddress);
    //transfer from callerAddress to targetAddress according to callValue

    if (callValue.compareTo(BigInteger.ZERO) > 0) {
      MUtil.transfer(rootRepository, callerAddress, contractAddress, callValue);
    }
    if (VMConfig.allowTvmTransferBrc10() && tokenValue.compareTo(BigInteger.ZERO) > 0) {
      MUtil.transferToken(rootRepository, callerAddress, contractAddress, String.valueOf(tokenId),
          tokenValue);
    }

  }

  public BigInteger getAccountEnergyLimitWithFixRatio(AccountCapsule account, BigInteger feeLimit, BigInteger callValue) {

    long sunPerEnergy = VMConstant.SUN_PER_ENERGY;
    if (rootRepository.getDynamicPropertiesStore().getEnergyFee() > 0) {
      sunPerEnergy = rootRepository.getDynamicPropertiesStore().getEnergyFee();
    }

    long leftFrozenEnergy = rootRepository.getAccountLeftEnergyFromFreeze(account);
    if (VMConfig.allowTvmFreeze() || VMConfig.allowTvmFreezeV2()) {
      receipt.setCallerEnergyLeft(leftFrozenEnergy);
    }

    BigInteger balanceTemp = account.getBalance().subtract(callValue);
    BigInteger energyFromBalance = (balanceTemp.compareTo(BigInteger.ZERO) > 0 ? balanceTemp : BigInteger.ZERO).divide(BigInteger.valueOf(sunPerEnergy));
    BigInteger availableEnergy = BigInteger.valueOf(leftFrozenEnergy).add(energyFromBalance);

    BigInteger energyFromFeeLimit = feeLimit.divide(BigInteger.valueOf(sunPerEnergy));
    if (VMConfig.allowTvmFreezeV2()) {
      long now = rootRepository.getHeadSlot();
      EnergyProcessor energyProcessor =
          new EnergyProcessor(
              rootRepository.getDynamicPropertiesStore(),
              ChainBaseManager.getInstance().getAccountStore());
      energyProcessor.updateUsage(account);
      account.setLatestConsumeTimeForEnergy(now);
      receipt.setCallerEnergyUsage(account.getEnergyUsage());
      receipt.setCallerEnergyWindowSize(account.getWindowSize(ENERGY));
      receipt.setCallerEnergyWindowSizeV2(account.getWindowSizeV2(ENERGY));
      account.setEnergyUsage(
          energyProcessor.increase(account, ENERGY,
              account.getEnergyUsage(), BigInteger.valueOf(leftFrozenEnergy).compareTo(energyFromFeeLimit) < 0 ? leftFrozenEnergy : energyFromFeeLimit.longValue(), now, now));
      receipt.setCallerEnergyMergedUsage(account.getEnergyUsage());
      receipt.setCallerEnergyMergedWindowSize(account.getWindowSize(ENERGY));
      rootRepository.updateAccount(account.createDbKey(), account);
    }
    return availableEnergy.compareTo(energyFromFeeLimit) < 0 ? availableEnergy : energyFromFeeLimit;

  }

  private BigInteger getAccountEnergyLimitWithFloatRatio(AccountCapsule account, BigInteger feeLimit,
      BigInteger callValue) {

    long sunPerEnergy = VMConstant.SUN_PER_ENERGY;
    if (rootRepository.getDynamicPropertiesStore().getEnergyFee() > 0) {
      sunPerEnergy = rootRepository.getDynamicPropertiesStore().getEnergyFee();
    }
    // can change the calc way
    long leftEnergyFromFreeze = rootRepository.getAccountLeftEnergyFromFreeze(account);
    callValue = callValue.compareTo(BigInteger.ZERO) > 0 ? callValue : BigInteger.ZERO;
    BigInteger balanceTemp = account.getBalance().subtract(callValue);
    BigInteger energyFromBalance = (balanceTemp.compareTo(BigInteger.ZERO) > 0 ? balanceTemp : BigInteger.ZERO).divide(BigInteger.valueOf(sunPerEnergy));

    BigInteger energyFromFeeLimit;
    BigInteger totalBalanceForEnergyFreeze = account.getAllFrozenBalanceForEnergy();
    if (totalBalanceForEnergyFreeze.compareTo(BigInteger.ZERO) == 0) {
      energyFromFeeLimit =
          feeLimit.divide(BigInteger.valueOf(sunPerEnergy));
    } else {
      long totalEnergyFromFreeze = rootRepository
          .calculateGlobalEnergyLimit(account);
      long leftBalanceForEnergyFreeze = getEnergyFee(totalBalanceForEnergyFreeze,
          BigInteger.valueOf(leftEnergyFromFreeze),
          BigInteger.valueOf(totalEnergyFromFreeze));

      if (BigInteger.valueOf(leftBalanceForEnergyFreeze).compareTo(feeLimit) >= 0) {
        energyFromFeeLimit = BigInteger.valueOf(totalEnergyFromFreeze)
            .multiply(feeLimit)
            .divide(totalBalanceForEnergyFreeze);
      } else {
        energyFromFeeLimit = BigInteger.valueOf(leftEnergyFromFreeze).add((feeLimit.subtract(BigInteger.valueOf(leftBalanceForEnergyFreeze))).divide(BigInteger.valueOf(sunPerEnergy)));
      }
    }

    BigInteger temp = BigInteger.valueOf(leftEnergyFromFreeze).add(energyFromBalance);
    return temp.compareTo(energyFromFeeLimit) < 0 ? temp : energyFromFeeLimit;
  }

  public BigInteger getTotalEnergyLimit(AccountCapsule creator, AccountCapsule caller,
      TriggerSmartContract contract, BigInteger feeLimit, BigInteger callValue)
      throws ContractValidateException {
    if (Objects.isNull(creator) && VMConfig.allowTvmConstantinople()) {
      return getAccountEnergyLimitWithFixRatio(caller, feeLimit, callValue);
    }
    //  according to version
    if (StorageUtils.getEnergyLimitHardFork()) {
      return getTotalEnergyLimitWithFixRatio(creator, caller, contract, feeLimit, callValue);
    } else {
      return getTotalEnergyLimitWithFloatRatio(creator, caller, contract, feeLimit, callValue);
    }
  }


  public void checkTokenValueAndId(BigInteger tokenValue, long tokenId) throws ContractValidateException {
    if (VMConfig.allowTvmTransferBrc10() && VMConfig.allowMultiSign()) {
      // tokenid can only be 0
      // or (MIN_TOKEN_ID, Long.Max]
      if (tokenId <= VMConstant.MIN_TOKEN_ID && tokenId != 0) {
        throw new ContractValidateException("tokenId must be > " + VMConstant.MIN_TOKEN_ID);
      }
      // tokenid can only be 0 when tokenvalue = 0,
      // or (MIN_TOKEN_ID, Long.Max]
      if (tokenValue.compareTo(BigInteger.ZERO) > 0 && tokenId == 0) {
        throw new ContractValidateException("invalid arguments with tokenValue = "
            + tokenValue + ", tokenId = " + tokenId);
      }
    }
  }


  private double getCpuLimitInUsRatio() {

    double cpuLimitRatio;

    if (ExecutorType.ET_NORMAL_TYPE == executorType) {
      // self witness generates block
      if (blockCap != null && blockCap.generatedByMyself
          && !blockCap.hasWitnessSignature()) {
        cpuLimitRatio = 1.0;
      } else {
        // self witness or other witness or fullnode verifies block
        if (bit.getRet(0).getContractRet() == contractResult.OUT_OF_TIME) {
          cpuLimitRatio = CommonParameter.getInstance().getMinTimeRatio();
        } else {
          cpuLimitRatio = CommonParameter.getInstance().getMaxTimeRatio();
        }
      }
    } else {
      // self witness or other witness or fullnode receives tx
      cpuLimitRatio = 1.0;
    }

    return cpuLimitRatio;
  }

  public BigInteger getTotalEnergyLimitWithFixRatio(AccountCapsule creator, AccountCapsule caller,
      TriggerSmartContract contract, BigInteger feeLimit, BigInteger callValue)
      throws ContractValidateException {

    BigInteger callerEnergyLimit = getAccountEnergyLimitWithFixRatio(caller, feeLimit, callValue);
    if (Arrays.equals(creator.getAddress().toByteArray(), caller.getAddress().toByteArray())) {
      // when the creator calls his own contract, this logic will be used.
      // so, the creator must use a BIG feeLimit to call his own contract,
      // which will cost the feeLimit BIT when the creator's frozen energy is 0.
      return callerEnergyLimit;
    }

    long creatorEnergyLimit = 0;
    ContractCapsule contractCapsule = rootRepository
        .getContract(contract.getContractAddress().toByteArray());
    long consumeUserResourcePercent = contractCapsule.getConsumeUserResourcePercent();

    long originEnergyLimit = contractCapsule.getOriginEnergyLimit();
    if (originEnergyLimit < 0) {
      throw new ContractValidateException("originEnergyLimit can't be < 0");
    }

    long originEnergyLeft = 0;
    if (consumeUserResourcePercent < VMConstant.ONE_HUNDRED) {
      originEnergyLeft = rootRepository.getAccountLeftEnergyFromFreeze(creator);
      if (VMConfig.allowTvmFreeze() || VMConfig.allowTvmFreezeV2()) {
        receipt.setOriginEnergyLeft(originEnergyLeft);
      }
    }
    if (consumeUserResourcePercent <= 0) {
      creatorEnergyLimit = min(originEnergyLeft, originEnergyLimit);
    } else {
      if (consumeUserResourcePercent < VMConstant.ONE_HUNDRED) {
        // creatorEnergyLimit =
        // min(callerEnergyLimit * (100 - percent) / percent,
        //   creatorLeftFrozenEnergy, originEnergyLimit)

        creatorEnergyLimit = min(
            callerEnergyLimit
                .multiply(BigInteger.valueOf(VMConstant.ONE_HUNDRED - consumeUserResourcePercent))
                .divide(BigInteger.valueOf(consumeUserResourcePercent)).longValueExact(),
            min(originEnergyLeft, originEnergyLimit)
        );
      }
    }
    if (VMConfig.allowTvmFreezeV2()) {
      long now = rootRepository.getHeadSlot();
      EnergyProcessor energyProcessor =
          new EnergyProcessor(
              rootRepository.getDynamicPropertiesStore(),
              ChainBaseManager.getInstance().getAccountStore());
      energyProcessor.updateUsage(creator);
      creator.setLatestConsumeTimeForEnergy(now);
      receipt.setOriginEnergyUsage(creator.getEnergyUsage());
      receipt.setOriginEnergyWindowSize(creator.getWindowSize(ENERGY));
      receipt.setOriginEnergyWindowSizeV2(creator.getWindowSizeV2(ENERGY));
      creator.setEnergyUsage(
          energyProcessor.increase(creator, ENERGY,
              creator.getEnergyUsage(), creatorEnergyLimit, now, now));
      receipt.setOriginEnergyMergedUsage(creator.getEnergyUsage());
      receipt.setOriginEnergyMergedWindowSize(creator.getWindowSize(ENERGY));
      rootRepository.updateAccount(creator.createDbKey(), creator);
    }
    return callerEnergyLimit.add(BigInteger.valueOf(creatorEnergyLimit));
  }

  private BigInteger getTotalEnergyLimitWithFloatRatio(AccountCapsule creator, AccountCapsule caller,
      TriggerSmartContract contract, BigInteger feeLimit, BigInteger callValue) {

    BigInteger callerEnergyLimit = getAccountEnergyLimitWithFloatRatio(caller, feeLimit, callValue);
    if (Arrays.equals(creator.getAddress().toByteArray(), caller.getAddress().toByteArray())) {
      return callerEnergyLimit;
    }

    // creatorEnergyFromFreeze
    long creatorEnergyLimit = rootRepository.getAccountLeftEnergyFromFreeze(creator);

    ContractCapsule contractCapsule = rootRepository
        .getContract(contract.getContractAddress().toByteArray());
    long consumeUserResourcePercent = contractCapsule.getConsumeUserResourcePercent();

    if (BigInteger.valueOf(creatorEnergyLimit * consumeUserResourcePercent)
            .compareTo((BigInteger.valueOf(VMConstant.ONE_HUNDRED).subtract(BigInteger.valueOf(consumeUserResourcePercent))).multiply(callerEnergyLimit)) > 0) {
      return (callerEnergyLimit.multiply(BigInteger.valueOf(VMConstant.ONE_HUNDRED))).divide(BigInteger.valueOf(consumeUserResourcePercent));
    } else {
      return callerEnergyLimit.add(BigInteger.valueOf(creatorEnergyLimit));
    }
  }

  private boolean isCheckTransaction() {
    return this.blockCap != null && !this.blockCap.getInstance().getBlockHeader()
        .getWitnessSignature().isEmpty();
  }

}
