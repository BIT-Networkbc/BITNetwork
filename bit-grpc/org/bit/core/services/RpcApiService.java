package org.bit.core.services;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolStringList;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.stub.StreamObserver;

import java.math.BigInteger;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.bit.api.DatabaseGrpc.DatabaseImplBase;
import org.bit.api.GrpcAPI;
import org.bit.api.GrpcAPI.AccountNetMessage;
import org.bit.api.GrpcAPI.AccountResourceMessage;
import org.bit.api.GrpcAPI.AssetIssueList;
import org.bit.api.GrpcAPI.BlockExtention;
import org.bit.api.GrpcAPI.BlockLimit;
import org.bit.api.GrpcAPI.BlockList;
import org.bit.api.GrpcAPI.BlockListExtention;
import org.bit.api.GrpcAPI.BlockReference;
import org.bit.api.GrpcAPI.BytesMessage;
import org.bit.api.GrpcAPI.CanWithdrawUnfreezeAmountRequestMessage;
import org.bit.api.GrpcAPI.DecryptNotes;
import org.bit.api.GrpcAPI.DecryptNotesMarked;
import org.bit.api.GrpcAPI.DecryptNotesBRC20;
import org.bit.api.GrpcAPI.DelegatedResourceList;
import org.bit.api.GrpcAPI.DelegatedResourceMessage;
import org.bit.api.GrpcAPI.DiversifierMessage;
import org.bit.api.GrpcAPI.EmptyMessage;
import org.bit.api.GrpcAPI.EstimateEnergyMessage;
import org.bit.api.GrpcAPI.ExchangeList;
import org.bit.api.GrpcAPI.ExpandedSpendingKeyMessage;
import org.bit.api.GrpcAPI.IncomingViewingKeyDiversifierMessage;
import org.bit.api.GrpcAPI.IncomingViewingKeyMessage;
import org.bit.api.GrpcAPI.IvkDecryptBRC20Parameters;
import org.bit.api.GrpcAPI.NfBRC20Parameters;
import org.bit.api.GrpcAPI.NodeList;
import org.bit.api.GrpcAPI.NoteParameters;
import org.bit.api.GrpcAPI.NumberMessage;
import org.bit.api.GrpcAPI.StringMessage;
import org.bit.api.GrpcAPI.OvkDecryptBRC20Parameters;
import org.bit.api.GrpcAPI.PaginatedMessage;
import org.bit.api.GrpcAPI.PaymentAddressMessage;
import org.bit.api.GrpcAPI.PricesResponseMessage;
import org.bit.api.GrpcAPI.PrivateParameters;
import org.bit.api.GrpcAPI.PrivateParametersWithoutAsk;
import org.bit.api.GrpcAPI.PrivateShieldedBRC20Parameters;
import org.bit.api.GrpcAPI.PrivateShieldedBRC20ParametersWithoutAsk;
import org.bit.api.GrpcAPI.ProposalList;
import org.bit.api.GrpcAPI.Return;
import org.bit.api.GrpcAPI.Return.response_code;
import org.bit.api.GrpcAPI.ShieldedAddressInfo;
import org.bit.api.GrpcAPI.ShieldedBRC20Parameters;
import org.bit.api.GrpcAPI.ShieldedBRC20TriggerContractParameters;
import org.bit.api.GrpcAPI.SpendAuthSigParameters;
import org.bit.api.GrpcAPI.SpendResult;
import org.bit.api.GrpcAPI.TransactionApprovedList;
import org.bit.api.GrpcAPI.TransactionExtention;
import org.bit.api.GrpcAPI.TransactionIdList;
import org.bit.api.GrpcAPI.TransactionInfoList;
import org.bit.api.GrpcAPI.TransactionList;
import org.bit.api.GrpcAPI.TransactionListExtention;
import org.bit.api.GrpcAPI.TransactionSignWeight;
import org.bit.api.GrpcAPI.ViewingKeyMessage;
import org.bit.api.GrpcAPI.WitnessList;
import org.bit.api.MonitorGrpc;
import org.bit.api.WalletExtensionGrpc;
import org.bit.api.WalletGrpc.WalletImplBase;
import org.bit.api.WalletSolidityGrpc.WalletSolidityImplBase;
import org.bit.common.application.RpcService;
import org.bit.common.es.ExecutorServiceManager;
import org.bit.common.parameter.CommonParameter;
import org.bit.common.utils.ByteArray;
import org.bit.common.utils.Sha256Hash;
import org.bit.common.utils.StringUtil;
import org.bit.core.ChainBaseManager;
import org.bit.core.Wallet;
import org.bit.core.capsule.AccountCapsule;
import org.bit.core.capsule.BlockCapsule;
import org.bit.core.capsule.TransactionCapsule;
import org.bit.core.capsule.WitnessCapsule;
import org.bit.core.config.args.Args;
import org.bit.core.db.Manager;
import org.bit.core.exception.BadItemException;
import org.bit.core.exception.ContractExeException;
import org.bit.core.exception.ContractValidateException;
import org.bit.core.exception.ItemNotFoundException;
import org.bit.core.exception.NonUniqueObjectException;
import org.bit.core.exception.StoreException;
import org.bit.core.exception.VMIllegalException;
import org.bit.core.exception.ZksnarkException;
import org.bit.core.metrics.MetricsApiService;
import org.bit.core.services.filter.LiteFnQueryGrpcInterceptor;
import org.bit.core.services.ratelimiter.RateLimiterInterceptor;
import org.bit.core.services.ratelimiter.RpcApiAccessInterceptor;
import org.bit.core.utils.TransactionUtil;
import org.bit.core.zen.address.DiversifierT;
import org.bit.core.zen.address.IncomingViewingKey;
import org.bit.protos.Protocol;
import org.bit.protos.Protocol.Account;
import org.bit.protos.Protocol.Block;
import org.bit.protos.Protocol.DynamicProperties;
import org.bit.protos.Protocol.Exchange;
import org.bit.protos.Protocol.MarketOrder;
import org.bit.protos.Protocol.MarketOrderList;
import org.bit.protos.Protocol.MarketOrderPair;
import org.bit.protos.Protocol.MarketOrderPairList;
import org.bit.protos.Protocol.MarketPriceList;
import org.bit.protos.Protocol.NodeInfo;
import org.bit.protos.Protocol.Proposal;
import org.bit.protos.Protocol.Transaction;
import org.bit.protos.Protocol.Transaction.Contract.ContractType;
import org.bit.protos.Protocol.TransactionInfo;
import org.bit.protos.contract.AccountContract.AccountCreateContract;
import org.bit.protos.contract.AccountContract.AccountPermissionUpdateContract;
import org.bit.protos.contract.AccountContract.AccountUpdateContract;
import org.bit.protos.contract.AccountContract.SetAccountIdContract;
import org.bit.protos.contract.AssetIssueContractOuterClass.AssetIssueContract;
import org.bit.protos.contract.AssetIssueContractOuterClass.ParticipateAssetIssueContract;
import org.bit.protos.contract.AssetIssueContractOuterClass.TransferAssetContract;
import org.bit.protos.contract.AssetIssueContractOuterClass.UnfreezeAssetContract;
import org.bit.protos.contract.AssetIssueContractOuterClass.UpdateAssetContract;
import org.bit.protos.contract.BalanceContract;
import org.bit.protos.contract.BalanceContract.AccountBalanceRequest;
import org.bit.protos.contract.BalanceContract.AccountBalanceResponse;
import org.bit.protos.contract.BalanceContract.BlockBalanceTrace;
import org.bit.protos.contract.BalanceContract.CancelAllUnfreezeV2Contract;
import org.bit.protos.contract.BalanceContract.DelegateResourceContract;
import org.bit.protos.contract.BalanceContract.FreezeBalanceContract;
import org.bit.protos.contract.BalanceContract.TransferContract;
import org.bit.protos.contract.BalanceContract.UnDelegateResourceContract;
import org.bit.protos.contract.BalanceContract.UnfreezeBalanceContract;
import org.bit.protos.contract.BalanceContract.WithdrawBalanceContract;
import org.bit.protos.contract.BalanceContract.WithdrawExpireUnfreezeContract;
import org.bit.protos.contract.ExchangeContract.ExchangeCreateContract;
import org.bit.protos.contract.ExchangeContract.ExchangeInjectContract;
import org.bit.protos.contract.ExchangeContract.ExchangeTransactionContract;
import org.bit.protos.contract.ExchangeContract.ExchangeWithdrawContract;
import org.bit.protos.contract.MarketContract.MarketCancelOrderContract;
import org.bit.protos.contract.MarketContract.MarketSellAssetContract;
import org.bit.protos.contract.ProposalContract.ProposalApproveContract;
import org.bit.protos.contract.ProposalContract.ProposalCreateContract;
import org.bit.protos.contract.ProposalContract.ProposalDeleteContract;
import org.bit.protos.contract.ShieldContract.IncrementalMerkleVoucherInfo;
import org.bit.protos.contract.ShieldContract.OutputPointInfo;
import org.bit.protos.contract.SmartContractOuterClass.ClearABIContract;
import org.bit.protos.contract.SmartContractOuterClass.CreateSmartContract;
import org.bit.protos.contract.SmartContractOuterClass.SmartContract;
import org.bit.protos.contract.SmartContractOuterClass.SmartContractDataWrapper;
import org.bit.protos.contract.SmartContractOuterClass.TriggerSmartContract;
import org.bit.protos.contract.SmartContractOuterClass.UpdateEnergyLimitContract;
import org.bit.protos.contract.SmartContractOuterClass.UpdateSettingContract;
import org.bit.protos.contract.StorageContract.UpdateBrokerageContract;
import org.bit.protos.contract.WitnessContract.VoteWitnessContract;
import org.bit.protos.contract.WitnessContract.WitnessCreateContract;
import org.bit.protos.contract.WitnessContract.WitnessUpdateContract;

@Component
@Slf4j(topic = "API")
public class RpcApiService extends RpcService {

  public static final String CONTRACT_VALIDATE_EXCEPTION = "ContractValidateException: {}";
  private static final String EXCEPTION_CAUGHT = "exception caught";
  private static final String UNKNOWN_EXCEPTION_CAUGHT = "unknown exception caught: ";
  private static final long BLOCK_LIMIT_NUM = 100;
  private static final long TRANSACTION_LIMIT_NUM = 1000;
  @Autowired
  private Manager dbManager;
  @Autowired
  private ChainBaseManager chainBaseManager;
  @Autowired
  private Wallet wallet;
  @Autowired
  private TransactionUtil transactionUtil;
  @Autowired
  private NodeInfoService nodeInfoService;
  @Autowired
  private RateLimiterInterceptor rateLimiterInterceptor;
  @Autowired
  private LiteFnQueryGrpcInterceptor liteFnQueryGrpcInterceptor;
  @Autowired
  private RpcApiAccessInterceptor apiAccessInterceptor;
  @Autowired
  private MetricsApiService metricsApiService;
  @Getter
  private DatabaseApi databaseApi = new DatabaseApi();
  private WalletApi walletApi = new WalletApi();
  @Getter
  private WalletSolidityApi walletSolidityApi = new WalletSolidityApi();
  @Getter
  private MonitorApi monitorApi = new MonitorApi();

  private final String executorName = "rpc-full-executor";

  @Override
  public void init() {

  }

  @Override
  public void init(CommonParameter args) {
    port = Args.getInstance().getRpcPort();
  }

  @Override
  public void start() {
    try {
      NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(port).addService(databaseApi);
      CommonParameter parameter = Args.getInstance();

      if (parameter.getRpcThreadNum() > 0) {
        serverBuilder = serverBuilder
            .executor(ExecutorServiceManager.newFixedThreadPool(
                executorName, parameter.getRpcThreadNum()));
      }

      if (parameter.isSolidityNode()) {
        serverBuilder = serverBuilder.addService(walletSolidityApi);
        if (parameter.isWalletExtensionApi()) {
          serverBuilder = serverBuilder.addService(new WalletExtensionApi());
        }
      } else {
        serverBuilder = serverBuilder.addService(walletApi);
      }

      if (parameter.isNodeMetricsEnable()) {
        serverBuilder = serverBuilder.addService(monitorApi);
      }

      // Set configs from config.conf or default value
      serverBuilder
          .maxConcurrentCallsPerConnection(parameter.getMaxConcurrentCallsPerConnection())
          .flowControlWindow(parameter.getFlowControlWindow())
          .maxConnectionIdle(parameter.getMaxConnectionIdleInMillis(), TimeUnit.MILLISECONDS)
          .maxConnectionAge(parameter.getMaxConnectionAgeInMillis(), TimeUnit.MILLISECONDS)
          .maxInboundMessageSize(parameter.getMaxMessageSize())
          .maxHeaderListSize(parameter.getMaxHeaderListSize());

      // add a rate limiter interceptor
      serverBuilder.intercept(rateLimiterInterceptor);

      // add api access interceptor
      serverBuilder.intercept(apiAccessInterceptor);

      // add lite fullnode query interceptor
      serverBuilder.intercept(liteFnQueryGrpcInterceptor);

      if (parameter.isRpcReflectionServiceEnable()) {
        serverBuilder.addService(ProtoReflectionService.newInstance());
      }

      apiServer = serverBuilder.build();
      rateLimiterInterceptor.init(apiServer);
      super.start();
    } catch (Exception e) {
      logger.debug(e.getMessage(), e);
    }
  }


  private void callContract(TriggerSmartContract request,
      StreamObserver<TransactionExtention> responseObserver, boolean isConstant) {
    TransactionExtention.Builder bitExtBuilder = TransactionExtention.newBuilder();
    Return.Builder retBuilder = Return.newBuilder();
    try {
      TransactionCapsule bitCap = createTransactionCapsule(request,
          ContractType.TriggerSmartContract);
      Transaction bit;
      if (isConstant) {
        bit = wallet.triggerConstantContract(request, bitCap, bitExtBuilder, retBuilder);
      } else {
        bit = wallet.triggerContract(request, bitCap, bitExtBuilder, retBuilder);
      }
      bitExtBuilder.setTransaction(bit);
      bitExtBuilder.setTxid(bitCap.getTransactionId().getByteString());
      retBuilder.setResult(true).setCode(response_code.SUCCESS);
      bitExtBuilder.setResult(retBuilder);
    } catch (ContractValidateException | VMIllegalException e) {
      retBuilder.setResult(false).setCode(response_code.CONTRACT_VALIDATE_ERROR)
          .setMessage(ByteString.copyFromUtf8(Wallet.CONTRACT_VALIDATE_ERROR + e.getMessage()));
      bitExtBuilder.setResult(retBuilder);
      logger.warn(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
    } catch (RuntimeException e) {
      retBuilder.setResult(false).setCode(response_code.CONTRACT_EXE_ERROR)
          .setMessage(ByteString.copyFromUtf8(e.getClass() + " : " + e.getMessage()));
      bitExtBuilder.setResult(retBuilder);
      logger.warn("When run constant call in VM, have RuntimeException: " + e.getMessage());
    } catch (Exception e) {
      retBuilder.setResult(false).setCode(response_code.OTHER_ERROR)
          .setMessage(ByteString.copyFromUtf8(e.getClass() + " : " + e.getMessage()));
      bitExtBuilder.setResult(retBuilder);
      logger.warn(UNKNOWN_EXCEPTION_CAUGHT + e.getMessage(), e);
    } finally {
      responseObserver.onNext(bitExtBuilder.build());
      responseObserver.onCompleted();
    }
  }

  private TransactionCapsule createTransactionCapsule(com.google.protobuf.Message message,
      ContractType contractType) throws ContractValidateException {
    return wallet.createTransactionCapsule(message, contractType);
  }


  private TransactionExtention transaction2Extention(Transaction transaction) {
    if (transaction == null) {
      return null;
    }
    TransactionExtention.Builder bitExtBuilder = TransactionExtention.newBuilder();
    Return.Builder retBuilder = Return.newBuilder();
    bitExtBuilder.setTransaction(transaction);
    bitExtBuilder.setTxid(Sha256Hash.of(CommonParameter.getInstance()
        .isECKeyCryptoEngine(), transaction.getRawData().toByteArray()).getByteString());
    retBuilder.setResult(true).setCode(response_code.SUCCESS);
    bitExtBuilder.setResult(retBuilder);
    return bitExtBuilder.build();
  }

  private BlockExtention block2Extention(Block block) {
    if (block == null) {
      return null;
    }
    BlockExtention.Builder builder = BlockExtention.newBuilder();
    BlockCapsule blockCapsule = new BlockCapsule(block);
    builder.setBlockHeader(block.getBlockHeader());
    builder.setBlockid(ByteString.copyFrom(blockCapsule.getBlockId().getBytes()));
    for (int i = 0; i < block.getTransactionsCount(); i++) {
      Transaction transaction = block.getTransactions(i);
      builder.addTransactions(transaction2Extention(transaction));
    }
    return builder.build();
  }

  private StatusRuntimeException getRunTimeException(Exception e) {
    if (e != null) {
      return Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException();
    } else {
      return Status.INTERNAL.withDescription("unknown").asRuntimeException();
    }
  }

  private void checkSupportShieldedTransaction() throws ZksnarkException {
    String msg = "Not support Shielded Transaction, need to be opened by the committee";
    if (!dbManager.getDynamicPropertiesStore().supportShieldedTransaction()) {
      throw new ZksnarkException(msg);
    }
  }

  private void checkSupportShieldedBRC20Transaction() throws ZksnarkException {
    String msg = "Not support Shielded BRC20 Transaction, need to be opened by the committee";
    if (!dbManager.getDynamicPropertiesStore().supportShieldedBRC20Transaction()) {
      throw new ZksnarkException(msg);
    }
  }

  /**
   * DatabaseApi.
   */
  public class DatabaseApi extends DatabaseImplBase {

    @Override
    public void getBlockReference(org.bit.api.GrpcAPI.EmptyMessage request,
        io.grpc.stub.StreamObserver<org.bit.api.GrpcAPI.BlockReference> responseObserver) {
      long headBlockNum = dbManager.getDynamicPropertiesStore()
          .getLatestBlockHeaderNumber();
      byte[] blockHeaderHash = dbManager.getDynamicPropertiesStore()
          .getLatestBlockHeaderHash().getBytes();
      BlockReference ref = BlockReference.newBuilder()
          .setBlockHash(ByteString.copyFrom(blockHeaderHash))
          .setBlockNum(headBlockNum)
          .build();
      responseObserver.onNext(ref);
      responseObserver.onCompleted();
    }

    @Override
    public void getNowBlock(EmptyMessage request, StreamObserver<Block> responseObserver) {
      Block block = null;
      try {
        block = chainBaseManager.getHead().getInstance();
      } catch (StoreException e) {
        logger.error(e.getMessage());
      }
      responseObserver.onNext(block);
      responseObserver.onCompleted();
    }

    @Override
    public void getBlockByNum(NumberMessage request, StreamObserver<Block> responseObserver) {
      Block block = null;
      try {
        block = chainBaseManager.getBlockByNum(request.getNum()).getInstance();
      } catch (StoreException e) {
        logger.error(e.getMessage());
      }
      responseObserver.onNext(block);
      responseObserver.onCompleted();
    }

    @Override
    public void getDynamicProperties(EmptyMessage request,
        StreamObserver<DynamicProperties> responseObserver) {
      DynamicProperties.Builder builder = DynamicProperties.newBuilder();
      builder.setLastSolidityBlockNum(
          dbManager.getDynamicPropertiesStore().getLatestSolidifiedBlockNum());
      DynamicProperties dynamicProperties = builder.build();
      responseObserver.onNext(dynamicProperties);
      responseObserver.onCompleted();
    }
  }

  /**
   * WalletSolidityApi.
   */
  public class WalletSolidityApi extends WalletSolidityImplBase {

    @Override
    public void getAccount(Account request, StreamObserver<Account> responseObserver) {
      ByteString addressBs = request.getAddress();
      if (addressBs != null) {
        Account reply = wallet.getAccount(request);
        responseObserver.onNext(reply);
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getAccountById(Account request, StreamObserver<Account> responseObserver) {
      ByteString id = request.getAccountId();
      if (id != null) {
        Account reply = wallet.getAccountById(request);
        responseObserver.onNext(reply);
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void listWitnesses(EmptyMessage request, StreamObserver<WitnessList> responseObserver) {
      responseObserver.onNext(wallet.getWitnessList());
      responseObserver.onCompleted();
    }

    @Override
    public void getAssetIssueList(EmptyMessage request,
        StreamObserver<AssetIssueList> responseObserver) {
      responseObserver.onNext(wallet.getAssetIssueList());
      responseObserver.onCompleted();
    }

    @Override
    public void getPaginatedAssetIssueList(PaginatedMessage request,
        StreamObserver<AssetIssueList> responseObserver) {
      responseObserver.onNext(wallet.getAssetIssueList(request.getOffset(), request.getLimit()));
      responseObserver.onCompleted();
    }

    @Override
    public void getAssetIssueByName(BytesMessage request,
        StreamObserver<AssetIssueContract> responseObserver) {
      ByteString assetName = request.getValue();
      if (assetName != null) {
        try {
          responseObserver.onNext(wallet.getAssetIssueByName(assetName));
        } catch (NonUniqueObjectException e) {
          responseObserver.onNext(null);
          logger.error("Solidity NonUniqueObjectException: {}", e.getMessage());
        }
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getAssetIssueListByName(BytesMessage request,
        StreamObserver<AssetIssueList> responseObserver) {
      ByteString assetName = request.getValue();

      if (assetName != null) {
        responseObserver.onNext(wallet.getAssetIssueListByName(assetName));
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getAssetIssueById(BytesMessage request,
        StreamObserver<AssetIssueContract> responseObserver) {
      ByteString assetId = request.getValue();

      if (assetId != null) {
        responseObserver.onNext(wallet.getAssetIssueById(assetId.toStringUtf8()));
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getNowBlock(EmptyMessage request, StreamObserver<Block> responseObserver) {
      responseObserver.onNext(wallet.getNowBlock());
      responseObserver.onCompleted();
    }

    @Override
    public void getNowBlock2(EmptyMessage request,
        StreamObserver<BlockExtention> responseObserver) {
      responseObserver.onNext(block2Extention(wallet.getNowBlock()));
      responseObserver.onCompleted();
    }

    @Override
    public void getBlockByNum(NumberMessage request, StreamObserver<Block> responseObserver) {
      long num = request.getNum();
      if (num >= 0) {
        Block reply = wallet.getBlockByNum(num);
        responseObserver.onNext(reply);
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getBlockByNum2(NumberMessage request,
        StreamObserver<BlockExtention> responseObserver) {
      long num = request.getNum();
      if (num >= 0) {
        Block reply = wallet.getBlockByNum(num);
        responseObserver.onNext(block2Extention(reply));
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }


    @Override
    public void getDelegatedResource(DelegatedResourceMessage request,
        StreamObserver<DelegatedResourceList> responseObserver) {
      responseObserver
          .onNext(wallet.getDelegatedResource(request.getFromAddress(), request.getToAddress()));
      responseObserver.onCompleted();
    }

    @Override
    public void getDelegatedResourceV2(DelegatedResourceMessage request,
        StreamObserver<DelegatedResourceList> responseObserver) {
      try {
        responseObserver.onNext(wallet.getDelegatedResourceV2(
                request.getFromAddress(), request.getToAddress())
        );
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getDelegatedResourceAccountIndex(BytesMessage request,
        StreamObserver<org.bit.protos.Protocol.DelegatedResourceAccountIndex> responseObserver) {
      try {
        responseObserver
          .onNext(wallet.getDelegatedResourceAccountIndex(request.getValue()));
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getDelegatedResourceAccountIndexV2(BytesMessage request,
        StreamObserver<org.bit.protos.Protocol.DelegatedResourceAccountIndex> responseObserver) {
      try {
        responseObserver
                .onNext(wallet.getDelegatedResourceAccountIndexV2(request.getValue()));
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getCanDelegatedMaxSize(GrpcAPI.CanDelegatedMaxSizeRequestMessage request,
        StreamObserver<GrpcAPI.CanDelegatedMaxSizeResponseMessage> responseObserver) {
      try {
        responseObserver.onNext(wallet.getCanDelegatedMaxSize(
                        request.getOwnerAddress(),request.getType()));
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getAvailableUnfreezeCount(GrpcAPI.GetAvailableUnfreezeCountRequestMessage request,
        StreamObserver<GrpcAPI.GetAvailableUnfreezeCountResponseMessage> responseObserver) {
      try {
        responseObserver.onNext(wallet.getAvailableUnfreezeCount(
                request.getOwnerAddress()));
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getCanWithdrawUnfreezeAmount(CanWithdrawUnfreezeAmountRequestMessage request,
        StreamObserver<GrpcAPI.CanWithdrawUnfreezeAmountResponseMessage> responseObserver) {
      try {
        responseObserver
                .onNext(wallet.getCanWithdrawUnfreezeAmount(
                        request.getOwnerAddress(), request.getTimestamp())
        );
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getExchangeById(BytesMessage request,
        StreamObserver<Exchange> responseObserver) {
      ByteString exchangeId = request.getValue();

      if (Objects.nonNull(exchangeId)) {
        responseObserver.onNext(wallet.getExchangeById(exchangeId));
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void listExchanges(EmptyMessage request,
        StreamObserver<ExchangeList> responseObserver) {
      responseObserver.onNext(wallet.getExchangeList());
      responseObserver.onCompleted();
    }

    @Override
    public void getTransactionCountByBlockNum(NumberMessage request,
        StreamObserver<NumberMessage> responseObserver) {
      getTransactionCountByBlockNumCommon(request, responseObserver);
    }

    @Override
    public void getTransactionById(BytesMessage request,
        StreamObserver<Transaction> responseObserver) {
      ByteString id = request.getValue();
      if (null != id) {
        Transaction reply = wallet.getTransactionById(id);

        responseObserver.onNext(reply);
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getTransactionInfoById(BytesMessage request,
        StreamObserver<TransactionInfo> responseObserver) {
      ByteString id = request.getValue();
      if (null != id) {
        TransactionInfo reply = wallet.getTransactionInfoById(id);

        responseObserver.onNext(reply);
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getRewardInfo(BytesMessage request,
        StreamObserver<StringMessage> responseObserver) {
      getRewardInfoCommon(request, responseObserver);
    }

    @Override
    public void getBrokerageInfo(BytesMessage request,
        StreamObserver<NumberMessage> responseObserver) {
      getBrokerageInfoCommon(request, responseObserver);
    }

    @Override
    public void getBurnBit(EmptyMessage request, StreamObserver<NumberMessage> responseObserver) {
      getBurnBitCommon(request, responseObserver);
    }

    @Override
    public void getMerkleTreeVoucherInfo(OutputPointInfo request,
        StreamObserver<IncrementalMerkleVoucherInfo> responseObserver) {

      try {
        checkSupportShieldedTransaction();

        IncrementalMerkleVoucherInfo witnessInfo = wallet
            .getMerkleTreeVoucherInfo(request);
        responseObserver.onNext(witnessInfo);
      } catch (Exception ex) {
        responseObserver.onError(getRunTimeException(ex));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void scanNoteByIvk(GrpcAPI.IvkDecryptParameters request,
        StreamObserver<GrpcAPI.DecryptNotes> responseObserver) {
      long startNum = request.getStartBlockIndex();
      long endNum = request.getEndBlockIndex();

      try {
        checkSupportShieldedTransaction();

        DecryptNotes decryptNotes = wallet
            .scanNoteByIvk(startNum, endNum, request.getIvk().toByteArray());
        responseObserver.onNext(decryptNotes);
      } catch (BadItemException | ZksnarkException e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void scanAndMarkNoteByIvk(GrpcAPI.IvkDecryptAndMarkParameters request,
        StreamObserver<GrpcAPI.DecryptNotesMarked> responseObserver) {
      long startNum = request.getStartBlockIndex();
      long endNum = request.getEndBlockIndex();

      try {
        checkSupportShieldedTransaction();

        DecryptNotesMarked decryptNotes = wallet.scanAndMarkNoteByIvk(startNum, endNum,
            request.getIvk().toByteArray(),
            request.getAk().toByteArray(),
            request.getNk().toByteArray());
        responseObserver.onNext(decryptNotes);
      } catch (BadItemException | ZksnarkException | InvalidProtocolBufferException
          | ItemNotFoundException e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void scanNoteByOvk(GrpcAPI.OvkDecryptParameters request,
        StreamObserver<GrpcAPI.DecryptNotes> responseObserver) {
      long startNum = request.getStartBlockIndex();
      long endNum = request.getEndBlockIndex();
      try {
        checkSupportShieldedTransaction();

        DecryptNotes decryptNotes = wallet
            .scanNoteByOvk(startNum, endNum, request.getOvk().toByteArray());
        responseObserver.onNext(decryptNotes);
      } catch (BadItemException | ZksnarkException e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void isSpend(NoteParameters request, StreamObserver<SpendResult> responseObserver) {
      try {
        checkSupportShieldedTransaction();

        responseObserver.onNext(wallet.isSpend(request));
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void scanShieldedBRC20NotesByIvk(IvkDecryptBRC20Parameters request,
        StreamObserver<DecryptNotesBRC20> responseObserver) {
      long startNum = request.getStartBlockIndex();
      long endNum = request.getEndBlockIndex();
      byte[] contractAddress = request.getShieldedBRC20ContractAddress().toByteArray();
      byte[] ivk = request.getIvk().toByteArray();
      byte[] ak = request.getAk().toByteArray();
      byte[] nk = request.getNk().toByteArray();
      ProtocolStringList topicsList = request.getEventsList();

      try {
        checkSupportShieldedBRC20Transaction();
        responseObserver.onNext(
            wallet.scanShieldedBRC20NotesByIvk(startNum, endNum, contractAddress, ivk, ak, nk,
                topicsList));

      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void scanShieldedBRC20NotesByOvk(OvkDecryptBRC20Parameters request,
        StreamObserver<DecryptNotesBRC20> responseObserver) {
      long startNum = request.getStartBlockIndex();
      long endNum = request.getEndBlockIndex();
      byte[] contractAddress = request.getShieldedBRC20ContractAddress().toByteArray();
      byte[] ovk = request.getOvk().toByteArray();
      ProtocolStringList topicList = request.getEventsList();
      try {
        checkSupportShieldedBRC20Transaction();
        responseObserver
            .onNext(wallet
                .scanShieldedBRC20NotesByOvk(startNum, endNum, ovk, contractAddress, topicList));
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void isShieldedBRC20ContractNoteSpent(NfBRC20Parameters request,
        StreamObserver<GrpcAPI.NullifierResult> responseObserver) {
      try {
        checkSupportShieldedBRC20Transaction();
        responseObserver.onNext(wallet.isShieldedBRC20ContractNoteSpent(request));
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getMarketOrderByAccount(BytesMessage request,
        StreamObserver<MarketOrderList> responseObserver) {
      try {
        ByteString address = request.getValue();

        MarketOrderList marketOrderList = wallet
            .getMarketOrderByAccount(address);
        responseObserver.onNext(marketOrderList);
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getMarketOrderById(BytesMessage request,
        StreamObserver<MarketOrder> responseObserver) {
      try {
        ByteString address = request.getValue();

        MarketOrder marketOrder = wallet
            .getMarketOrderById(address);
        responseObserver.onNext(marketOrder);
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getMarketPriceByPair(MarketOrderPair request,
        StreamObserver<MarketPriceList> responseObserver) {
      try {
        MarketPriceList marketPriceList = wallet
            .getMarketPriceByPair(request.getSellTokenId().toByteArray(),
                request.getBuyTokenId().toByteArray());
        responseObserver.onNext(marketPriceList);
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getMarketOrderListByPair(org.bit.protos.Protocol.MarketOrderPair request,
        StreamObserver<MarketOrderList> responseObserver) {
      try {
        MarketOrderList orderPairList = wallet
            .getMarketOrderListByPair(request.getSellTokenId().toByteArray(),
                request.getBuyTokenId().toByteArray());
        responseObserver.onNext(orderPairList);
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getMarketPairList(EmptyMessage request,
        StreamObserver<MarketOrderPairList> responseObserver) {
      try {
        MarketOrderPairList pairList = wallet.getMarketPairList();
        responseObserver.onNext(pairList);
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void triggerConstantContract(TriggerSmartContract request,
        StreamObserver<TransactionExtention> responseObserver) {

      callContract(request, responseObserver, true);
    }

    @Override
    public void estimateEnergy(TriggerSmartContract request,
        StreamObserver<EstimateEnergyMessage> responseObserver) {
      TransactionExtention.Builder bitExtBuilder = TransactionExtention.newBuilder();
      Return.Builder retBuilder = Return.newBuilder();
      EstimateEnergyMessage.Builder estimateBuilder
          = EstimateEnergyMessage.newBuilder();

      try {
        TransactionCapsule bitCap = createTransactionCapsule(request,
            ContractType.TriggerSmartContract);
        wallet.estimateEnergy(request, bitCap, bitExtBuilder, retBuilder, estimateBuilder);
      } catch (ContractValidateException | VMIllegalException e) {
        retBuilder.setResult(false).setCode(response_code.CONTRACT_VALIDATE_ERROR)
            .setMessage(ByteString.copyFromUtf8(Wallet
                .CONTRACT_VALIDATE_ERROR + e.getMessage()));
        logger.warn(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      } catch (RuntimeException e) {
        retBuilder.setResult(false).setCode(response_code.CONTRACT_EXE_ERROR)
            .setMessage(ByteString.copyFromUtf8(e.getClass() + " : " + e.getMessage()));
        logger.warn("When run estimate energy in VM, have Runtime Exception: " + e.getMessage());
      } catch (Exception e) {
        retBuilder.setResult(false).setCode(response_code.OTHER_ERROR)
            .setMessage(ByteString.copyFromUtf8(e.getClass() + " : " + e.getMessage()));
        logger.warn(UNKNOWN_EXCEPTION_CAUGHT + e.getMessage(), e);
      } finally {
        estimateBuilder.setResult(retBuilder);
        responseObserver.onNext(estimateBuilder.build());
        responseObserver.onCompleted();
      }
    }

    @Override
    public void getTransactionInfoByBlockNum(NumberMessage request,
        StreamObserver<TransactionInfoList> responseObserver) {
      try {
        responseObserver.onNext(wallet.getTransactionInfoByBlockNum(request.getNum()));
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }

      responseObserver.onCompleted();
    }

    @Override
    public void getBlock(GrpcAPI.BlockReq  request,
        StreamObserver<BlockExtention> responseObserver) {
      getBlockCommon(request, responseObserver);
    }

    @Override
    public void getBandwidthPrices(EmptyMessage request,
        StreamObserver<PricesResponseMessage> responseObserver) {
      try {
        responseObserver.onNext(wallet.getBandwidthPrices());
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getEnergyPrices(EmptyMessage request,
        StreamObserver<PricesResponseMessage> responseObserver) {
      try {
        responseObserver.onNext(wallet.getEnergyPrices());
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }
  }

  /**
   * WalletExtensionApi.
   */
  public class WalletExtensionApi extends WalletExtensionGrpc.WalletExtensionImplBase {

    private TransactionListExtention transactionList2Extention(TransactionList transactionList) {
      if (transactionList == null) {
        return null;
      }
      TransactionListExtention.Builder builder = TransactionListExtention.newBuilder();
      for (Transaction transaction : transactionList.getTransactionList()) {
        builder.addTransaction(transaction2Extention(transaction));
      }
      return builder.build();
    }
  }

  /**
   * WalletApi.
   */
  public class WalletApi extends WalletImplBase {

    private BlockListExtention blockList2Extention(BlockList blockList) {
      if (blockList == null) {
        return null;
      }
      BlockListExtention.Builder builder = BlockListExtention.newBuilder();
      for (Block block : blockList.getBlockList()) {
        builder.addBlock(block2Extention(block));
      }
      return builder.build();
    }

    @Override
    public void getAccount(Account req, StreamObserver<Account> responseObserver) {
      ByteString addressBs = req.getAddress();
      if (addressBs != null) {
        Account reply = wallet.getAccount(req);
        responseObserver.onNext(reply);
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getAccountById(Account req, StreamObserver<Account> responseObserver) {
      ByteString accountId = req.getAccountId();
      if (accountId != null) {
        Account reply = wallet.getAccountById(req);
        responseObserver.onNext(reply);
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    /**
     *
     */
    public void getAccountBalance(AccountBalanceRequest request,
        StreamObserver<AccountBalanceResponse> responseObserver) {
      try {
        AccountBalanceResponse accountBalanceResponse = wallet.getAccountBalance(request);
        responseObserver.onNext(accountBalanceResponse);
        responseObserver.onCompleted();
      } catch (Exception e) {
        responseObserver.onError(e);
      }
    }

    /**
     *
     */
    public void getBlockBalanceTrace(BlockBalanceTrace.BlockIdentifier request,
        StreamObserver<BlockBalanceTrace> responseObserver) {
      try {
        BlockBalanceTrace blockBalanceTrace = wallet.getBlockBalance(request);
        responseObserver.onNext(blockBalanceTrace);
        responseObserver.onCompleted();
      } catch (Exception e) {
        responseObserver.onError(e);
      }
    }

    @Override
    public void createTransaction(TransferContract request,
        StreamObserver<Transaction> responseObserver) {
      try {
        responseObserver
            .onNext(
                createTransactionCapsule(request, ContractType.TransferContract).getInstance());
      } catch (ContractValidateException e) {
        responseObserver
            .onNext(null);
        logger.debug(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      }
      responseObserver.onCompleted();
    }

    @Override
    public void createTransaction2(TransferContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.TransferContract, responseObserver);
    }

    private void createTransactionExtention(Message request, ContractType contractType,
        StreamObserver<TransactionExtention> responseObserver) {
      TransactionExtention.Builder bitExtBuilder = TransactionExtention.newBuilder();
      Return.Builder retBuilder = Return.newBuilder();
      try {
        TransactionCapsule bit = createTransactionCapsule(request, contractType);
        bitExtBuilder.setTransaction(bit.getInstance());
        bitExtBuilder.setTxid(bit.getTransactionId().getByteString());
        retBuilder.setResult(true).setCode(response_code.SUCCESS);
      } catch (ContractValidateException e) {
        retBuilder.setResult(false).setCode(response_code.CONTRACT_VALIDATE_ERROR)
            .setMessage(ByteString
                .copyFromUtf8(Wallet.CONTRACT_VALIDATE_ERROR + e.getMessage()));
        logger.debug(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      } catch (Exception e) {
        retBuilder.setResult(false).setCode(response_code.OTHER_ERROR)
            .setMessage(ByteString.copyFromUtf8(e.getClass() + " : " + e.getMessage()));
        logger.info(EXCEPTION_CAUGHT + e.getMessage());
      }
      bitExtBuilder.setResult(retBuilder);
      responseObserver.onNext(bitExtBuilder.build());
      responseObserver.onCompleted();
    }

    @Override
    public void getTransactionSignWeight(Transaction req,
        StreamObserver<TransactionSignWeight> responseObserver) {
      TransactionSignWeight tsw = transactionUtil.getTransactionSignWeight(req);
      responseObserver.onNext(tsw);
      responseObserver.onCompleted();
    }

    @Override
    public void getTransactionApprovedList(Transaction req,
        StreamObserver<TransactionApprovedList> responseObserver) {
      TransactionApprovedList tal = wallet.getTransactionApprovedList(req);
      responseObserver.onNext(tal);
      responseObserver.onCompleted();
    }

    @Override
    public void broadcastTransaction(Transaction req,
        StreamObserver<GrpcAPI.Return> responseObserver) {
      GrpcAPI.Return result = wallet.broadcastTransaction(req);
      responseObserver.onNext(result);
      responseObserver.onCompleted();
    }

    @Override
    public void createAssetIssue(AssetIssueContract request,
        StreamObserver<Transaction> responseObserver) {
      try {
        responseObserver.onNext(
            createTransactionCapsule(request, ContractType.AssetIssueContract).getInstance());
      } catch (ContractValidateException e) {
        responseObserver.onNext(null);
        logger.debug(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      }
      responseObserver.onCompleted();
    }

    @Override
    public void createAssetIssue2(AssetIssueContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.AssetIssueContract, responseObserver);
    }

    @Override
    public void unfreezeAsset(UnfreezeAssetContract request,
        StreamObserver<Transaction> responseObserver) {
      try {
        responseObserver.onNext(
            createTransactionCapsule(request, ContractType.UnfreezeAssetContract).getInstance());
      } catch (ContractValidateException e) {
        responseObserver.onNext(null);
        logger.debug(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      }
      responseObserver.onCompleted();
    }

    @Override
    public void unfreezeAsset2(UnfreezeAssetContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.UnfreezeAssetContract, responseObserver);
    }

    //refactor、test later
    private void checkVoteWitnessAccount(VoteWitnessContract req) {
      //send back to cli
      ByteString ownerAddress = req.getOwnerAddress();
      Preconditions.checkNotNull(ownerAddress, "OwnerAddress is null");

      AccountCapsule account = dbManager.getAccountStore().get(ownerAddress.toByteArray());
      Preconditions.checkNotNull(account,
          "OwnerAddress[" + StringUtil.createReadableString(ownerAddress) + "] not exists");

      int votesCount = req.getVotesCount();
      Preconditions.checkArgument(votesCount <= 0, "VotesCount[" + votesCount + "] <= 0");
      if (dbManager.getDynamicPropertiesStore().supportAllowNewResourceModel()) {
        Preconditions.checkArgument(account.getAllBitPower().compareTo(BigInteger.valueOf(votesCount)) < 0,
            "bit power[" + account.getAllBitPower() + "] <  VotesCount[" + votesCount + "]");
      } else {
        Preconditions.checkArgument(account.getBitPower().compareTo(BigInteger.valueOf(votesCount)) < 0,
            "bit power[" + account.getBitPower() + "] <  VotesCount[" + votesCount + "]");
      }

      req.getVotesList().forEach(vote -> {
        ByteString voteAddress = vote.getVoteAddress();
        WitnessCapsule witness = dbManager.getWitnessStore()
            .get(voteAddress.toByteArray());
        String readableWitnessAddress = StringUtil.createReadableString(voteAddress);

        Preconditions.checkNotNull(witness, "witness[" + readableWitnessAddress + "] not exists");
        Preconditions.checkArgument(vote.getVoteCount() <= 0,
            "VoteAddress[" + readableWitnessAddress + "], VotesCount[" + vote
                .getVoteCount() + "] <= 0");
      });
    }

    @Override
    public void voteWitnessAccount(VoteWitnessContract request,
        StreamObserver<Transaction> responseObserver) {
      try {
        responseObserver.onNext(
            createTransactionCapsule(request, ContractType.VoteWitnessContract).getInstance());
      } catch (ContractValidateException e) {
        responseObserver
            .onNext(null);
        logger.debug(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      }
      responseObserver.onCompleted();
    }

    @Override
    public void voteWitnessAccount2(VoteWitnessContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.VoteWitnessContract, responseObserver);
    }

    @Override
    public void updateSetting(UpdateSettingContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.UpdateSettingContract,
          responseObserver);
    }

    @Override
    public void updateEnergyLimit(UpdateEnergyLimitContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.UpdateEnergyLimitContract,
          responseObserver);
    }

    @Override
    public void clearContractABI(ClearABIContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.ClearABIContract,
          responseObserver);
    }

    @Override
    public void createWitness(WitnessCreateContract request,
        StreamObserver<Transaction> responseObserver) {
      try {
        responseObserver.onNext(
            createTransactionCapsule(request, ContractType.WitnessCreateContract).getInstance());
      } catch (ContractValidateException e) {
        responseObserver
            .onNext(null);
        logger.debug(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      }
      responseObserver.onCompleted();
    }

    @Override
    public void createWitness2(WitnessCreateContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.WitnessCreateContract, responseObserver);
    }

    @Override
    public void createAccount(AccountCreateContract request,
        StreamObserver<Transaction> responseObserver) {
      try {
        responseObserver.onNext(
            createTransactionCapsule(request, ContractType.AccountCreateContract).getInstance());
      } catch (ContractValidateException e) {
        responseObserver
            .onNext(null);
        logger.debug(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      }
      responseObserver.onCompleted();
    }

    @Override
    public void createAccount2(AccountCreateContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.AccountCreateContract, responseObserver);
    }

    @Override
    public void updateWitness(WitnessUpdateContract request,
        StreamObserver<Transaction> responseObserver) {
      try {
        responseObserver.onNext(
            createTransactionCapsule(request, ContractType.WitnessUpdateContract).getInstance());
      } catch (ContractValidateException e) {
        responseObserver
            .onNext(null);
        logger.debug(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      }
      responseObserver.onCompleted();
    }

    @Override
    public void updateWitness2(WitnessUpdateContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.WitnessUpdateContract, responseObserver);
    }

    @Override
    public void updateAccount(AccountUpdateContract request,
        StreamObserver<Transaction> responseObserver) {
      try {
        responseObserver.onNext(
            createTransactionCapsule(request, ContractType.AccountUpdateContract).getInstance());
      } catch (ContractValidateException e) {
        responseObserver
            .onNext(null);
        logger.debug(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      }
      responseObserver.onCompleted();
    }

    @Override
    public void setAccountId(SetAccountIdContract request,
        StreamObserver<Transaction> responseObserver) {
      try {
        responseObserver.onNext(
            createTransactionCapsule(request, ContractType.SetAccountIdContract).getInstance());
      } catch (ContractValidateException e) {
        responseObserver
            .onNext(null);
        logger.debug(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      }
      responseObserver.onCompleted();
    }

    @Override
    public void updateAccount2(AccountUpdateContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.AccountUpdateContract, responseObserver);
    }

    @Override
    public void updateAsset(UpdateAssetContract request,
        StreamObserver<Transaction> responseObserver) {
      try {
        responseObserver.onNext(
            createTransactionCapsule(request,
                ContractType.UpdateAssetContract).getInstance());
      } catch (ContractValidateException e) {
        responseObserver
            .onNext(null);
        logger.debug("ContractValidateException", e.getMessage());
      }
      responseObserver.onCompleted();
    }

    @Override
    public void updateAsset2(UpdateAssetContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.UpdateAssetContract, responseObserver);
    }

    @Override
    public void freezeBalance(FreezeBalanceContract request,
        StreamObserver<Transaction> responseObserver) {
      try {
        responseObserver.onNext(
            createTransactionCapsule(request, ContractType.FreezeBalanceContract).getInstance());
      } catch (ContractValidateException e) {
        responseObserver
            .onNext(null);
        logger.debug(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      }
      responseObserver.onCompleted();
    }

    @Override
    public void freezeBalance2(FreezeBalanceContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.FreezeBalanceContract, responseObserver);
    }

    @Override
    public void freezeBalanceV2(BalanceContract.FreezeBalanceV2Contract request,
                                StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.FreezeBalanceV2Contract, responseObserver);
    }

    @Override
    public void unfreezeBalance(UnfreezeBalanceContract request,
        StreamObserver<Transaction> responseObserver) {
      try {
        responseObserver.onNext(
            createTransactionCapsule(request, ContractType.UnfreezeBalanceContract)
                .getInstance());
      } catch (ContractValidateException e) {
        responseObserver
            .onNext(null);
        logger.debug(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      }
      responseObserver.onCompleted();
    }

    @Override
    public void unfreezeBalance2(UnfreezeBalanceContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.UnfreezeBalanceContract, responseObserver);
    }

    @Override
    public void unfreezeBalanceV2(BalanceContract.UnfreezeBalanceV2Contract request,
                                  StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.UnfreezeBalanceV2Contract, responseObserver);
    }

    @Override
    public void withdrawBalance(WithdrawBalanceContract request,
        StreamObserver<Transaction> responseObserver) {
      try {
        responseObserver.onNext(
            createTransactionCapsule(request, ContractType.WithdrawBalanceContract)
                .getInstance());
      } catch (ContractValidateException e) {
        responseObserver
            .onNext(null);
        logger.debug(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      }
      responseObserver.onCompleted();
    }

    @Override
    public void withdrawBalance2(WithdrawBalanceContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.WithdrawBalanceContract, responseObserver);
    }

    @Override
    public void withdrawExpireUnfreeze(WithdrawExpireUnfreezeContract request,
                                       StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.WithdrawExpireUnfreezeContract,
              responseObserver);
    }

    @Override
    public void delegateResource(DelegateResourceContract request,
                                 StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.DelegateResourceContract,
          responseObserver);
    }

    @Override
    public void unDelegateResource(UnDelegateResourceContract request,
                                       StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.UnDelegateResourceContract,
          responseObserver);
    }

    @Override
    public void cancelAllUnfreezeV2(CancelAllUnfreezeV2Contract request,
                                    StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.CancelAllUnfreezeV2Contract,
          responseObserver);
    }

    @Override
    public void proposalCreate(ProposalCreateContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.ProposalCreateContract, responseObserver);
    }


    @Override
    public void proposalApprove(ProposalApproveContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.ProposalApproveContract, responseObserver);
    }

    @Override
    public void proposalDelete(ProposalDeleteContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.ProposalDeleteContract, responseObserver);
    }

    @Override
    public void exchangeCreate(ExchangeCreateContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.ExchangeCreateContract, responseObserver);
    }


    @Override
    public void exchangeInject(ExchangeInjectContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.ExchangeInjectContract, responseObserver);
    }

    @Override
    public void exchangeWithdraw(ExchangeWithdrawContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.ExchangeWithdrawContract, responseObserver);
    }

    @Override
    public void exchangeTransaction(ExchangeTransactionContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.ExchangeTransactionContract,
          responseObserver);
    }

    @Override
    public void getNowBlock(EmptyMessage request, StreamObserver<Block> responseObserver) {
      responseObserver.onNext(wallet.getNowBlock());
      responseObserver.onCompleted();
    }

    @Override
    public void getNowBlock2(EmptyMessage request,
        StreamObserver<BlockExtention> responseObserver) {
      Block block = wallet.getNowBlock();
      responseObserver.onNext(block2Extention(block));
      responseObserver.onCompleted();
    }

    @Override
    public void getBlockByNum(NumberMessage request, StreamObserver<Block> responseObserver) {
      responseObserver.onNext(wallet.getBlockByNum(request.getNum()));
      responseObserver.onCompleted();
    }

    @Override
    public void getBlockByNum2(NumberMessage request,
        StreamObserver<BlockExtention> responseObserver) {
      Block block = wallet.getBlockByNum(request.getNum());
      responseObserver.onNext(block2Extention(block));
      responseObserver.onCompleted();
    }

    @Override
    public void getTransactionCountByBlockNum(NumberMessage request,
        StreamObserver<NumberMessage> responseObserver) {
      getTransactionCountByBlockNumCommon(request, responseObserver);
    }

    @Override
    public void listNodes(EmptyMessage request, StreamObserver<NodeList> responseObserver) {
      responseObserver.onNext(wallet.listNodes());
      responseObserver.onCompleted();
    }

    @Override
    public void transferAsset(TransferAssetContract request,
        StreamObserver<Transaction> responseObserver) {
      try {
        responseObserver
            .onNext(createTransactionCapsule(request, ContractType.TransferAssetContract)
                .getInstance());
      } catch (ContractValidateException e) {
        responseObserver
            .onNext(null);
        logger.debug(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      }
      responseObserver.onCompleted();
    }

    @Override
    public void transferAsset2(TransferAssetContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.TransferAssetContract, responseObserver);
    }

    @Override
    public void participateAssetIssue(ParticipateAssetIssueContract request,
        StreamObserver<Transaction> responseObserver) {
      try {
        responseObserver
            .onNext(createTransactionCapsule(request, ContractType.ParticipateAssetIssueContract)
                .getInstance());
      } catch (ContractValidateException e) {
        responseObserver
            .onNext(null);
        logger.debug(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      }
      responseObserver.onCompleted();
    }

    @Override
    public void participateAssetIssue2(ParticipateAssetIssueContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.ParticipateAssetIssueContract,
          responseObserver);
    }

    @Override
    public void getAssetIssueByAccount(Account request,
        StreamObserver<AssetIssueList> responseObserver) {
      ByteString fromBs = request.getAddress();

      if (fromBs != null) {
        responseObserver.onNext(wallet.getAssetIssueByAccount(fromBs));
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getAccountNet(Account request,
        StreamObserver<AccountNetMessage> responseObserver) {
      ByteString fromBs = request.getAddress();

      if (fromBs != null) {
        responseObserver.onNext(wallet.getAccountNet(fromBs));
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getAccountResource(Account request,
        StreamObserver<AccountResourceMessage> responseObserver) {
      ByteString fromBs = request.getAddress();

      if (fromBs != null) {
        responseObserver.onNext(wallet.getAccountResource(fromBs));
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getAssetIssueByName(BytesMessage request,
        StreamObserver<AssetIssueContract> responseObserver) {
      ByteString assetName = request.getValue();
      if (assetName != null) {
        try {
          responseObserver.onNext(wallet.getAssetIssueByName(assetName));
        } catch (NonUniqueObjectException e) {
          responseObserver.onNext(null);
          logger.debug("FullNode NonUniqueObjectException: {}", e.getMessage());
        }
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getAssetIssueListByName(BytesMessage request,
        StreamObserver<AssetIssueList> responseObserver) {
      ByteString assetName = request.getValue();

      if (assetName != null) {
        responseObserver.onNext(wallet.getAssetIssueListByName(assetName));
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getAssetIssueById(BytesMessage request,
        StreamObserver<AssetIssueContract> responseObserver) {
      ByteString assetId = request.getValue();

      if (assetId != null) {
        responseObserver.onNext(wallet.getAssetIssueById(assetId.toStringUtf8()));
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getBlockById(BytesMessage request, StreamObserver<Block> responseObserver) {
      ByteString blockId = request.getValue();

      if (Objects.nonNull(blockId)) {
        responseObserver.onNext(wallet.getBlockById(blockId));
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getProposalById(BytesMessage request,
        StreamObserver<Proposal> responseObserver) {
      ByteString proposalId = request.getValue();

      if (Objects.nonNull(proposalId)) {
        responseObserver.onNext(wallet.getProposalById(proposalId));
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getExchangeById(BytesMessage request,
        StreamObserver<Exchange> responseObserver) {
      ByteString exchangeId = request.getValue();

      if (Objects.nonNull(exchangeId)) {
        responseObserver.onNext(wallet.getExchangeById(exchangeId));
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getBlockByLimitNext(BlockLimit request,
        StreamObserver<BlockList> responseObserver) {
      long startNum = request.getStartNum();
      long endNum = request.getEndNum();

      if (endNum > 0 && endNum > startNum && endNum - startNum <= BLOCK_LIMIT_NUM) {
        responseObserver.onNext(wallet.getBlocksByLimitNext(startNum, endNum - startNum));
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getBlockByLimitNext2(BlockLimit request,
        StreamObserver<BlockListExtention> responseObserver) {
      long startNum = request.getStartNum();
      long endNum = request.getEndNum();

      if (endNum > 0 && endNum > startNum && endNum - startNum <= BLOCK_LIMIT_NUM) {
        responseObserver
            .onNext(blockList2Extention(wallet.getBlocksByLimitNext(startNum, endNum - startNum)));
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getBlockByLatestNum(NumberMessage request,
        StreamObserver<BlockList> responseObserver) {
      long getNum = request.getNum();

      if (getNum > 0 && getNum < BLOCK_LIMIT_NUM) {
        responseObserver.onNext(wallet.getBlockByLatestNum(getNum));
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getBlockByLatestNum2(NumberMessage request,
        StreamObserver<BlockListExtention> responseObserver) {
      long getNum = request.getNum();

      if (getNum > 0 && getNum < BLOCK_LIMIT_NUM) {
        responseObserver.onNext(blockList2Extention(wallet.getBlockByLatestNum(getNum)));
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getTransactionById(BytesMessage request,
        StreamObserver<Transaction> responseObserver) {
      ByteString transactionId = request.getValue();

      if (Objects.nonNull(transactionId)) {
        responseObserver.onNext(wallet.getTransactionById(transactionId));
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void deployContract(CreateSmartContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.CreateSmartContract, responseObserver);
    }

    public void totalTransaction(EmptyMessage request,
        StreamObserver<NumberMessage> responseObserver) {
      responseObserver.onNext(wallet.totalTransaction());
      responseObserver.onCompleted();
    }

    @Override
    public void getNextMaintenanceTime(EmptyMessage request,
        StreamObserver<NumberMessage> responseObserver) {
      responseObserver.onNext(wallet.getNextMaintenanceTime());
      responseObserver.onCompleted();
    }

    @Override
    public void getAssetIssueList(EmptyMessage request,
        StreamObserver<AssetIssueList> responseObserver) {
      responseObserver.onNext(wallet.getAssetIssueList());
      responseObserver.onCompleted();
    }

    @Override
    public void triggerContract(TriggerSmartContract request,
        StreamObserver<TransactionExtention> responseObserver) {

      callContract(request, responseObserver, false);
    }

    @Override
    public void estimateEnergy(TriggerSmartContract request,
        StreamObserver<EstimateEnergyMessage> responseObserver) {
      TransactionExtention.Builder bitExtBuilder = TransactionExtention.newBuilder();
      Return.Builder retBuilder = Return.newBuilder();
      EstimateEnergyMessage.Builder estimateBuilder
          = EstimateEnergyMessage.newBuilder();

      try {
        TransactionCapsule bitCap = createTransactionCapsule(request,
            ContractType.TriggerSmartContract);
        wallet.estimateEnergy(request, bitCap, bitExtBuilder, retBuilder, estimateBuilder);
      } catch (ContractValidateException | VMIllegalException e) {
        retBuilder.setResult(false).setCode(response_code.CONTRACT_VALIDATE_ERROR)
            .setMessage(ByteString.copyFromUtf8(Wallet
                .CONTRACT_VALIDATE_ERROR + e.getMessage()));
        logger.warn(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      } catch (RuntimeException e) {
        retBuilder.setResult(false).setCode(response_code.CONTRACT_EXE_ERROR)
            .setMessage(ByteString.copyFromUtf8(e.getClass() + " : " + e.getMessage()));
        logger.warn("When run estimate energy in VM, have Runtime Exception: " + e.getMessage());
      } catch (Exception e) {
        retBuilder.setResult(false).setCode(response_code.OTHER_ERROR)
            .setMessage(ByteString.copyFromUtf8(e.getClass() + " : " + e.getMessage()));
        logger.warn(UNKNOWN_EXCEPTION_CAUGHT + e.getMessage(), e);
      } finally {
        estimateBuilder.setResult(retBuilder);
        responseObserver.onNext(estimateBuilder.build());
        responseObserver.onCompleted();
      }
    }

    @Override
    public void triggerConstantContract(TriggerSmartContract request,
        StreamObserver<TransactionExtention> responseObserver) {

      callContract(request, responseObserver, true);
    }

    private void callContract(TriggerSmartContract request,
        StreamObserver<TransactionExtention> responseObserver, boolean isConstant) {
      TransactionExtention.Builder bitExtBuilder = TransactionExtention.newBuilder();
      Return.Builder retBuilder = Return.newBuilder();
      try {
        TransactionCapsule bitCap = createTransactionCapsule(request,
            ContractType.TriggerSmartContract);
        Transaction bit;
        if (isConstant) {
          bit = wallet.triggerConstantContract(request, bitCap, bitExtBuilder, retBuilder);
        } else {
          bit = wallet.triggerContract(request, bitCap, bitExtBuilder, retBuilder);
        }
        bitExtBuilder.setTransaction(bit);
        bitExtBuilder.setTxid(bitCap.getTransactionId().getByteString());
        retBuilder.setResult(true).setCode(response_code.SUCCESS);
        bitExtBuilder.setResult(retBuilder);
      } catch (ContractValidateException | VMIllegalException e) {
        retBuilder.setResult(false).setCode(response_code.CONTRACT_VALIDATE_ERROR)
            .setMessage(ByteString.copyFromUtf8(Wallet
                .CONTRACT_VALIDATE_ERROR + e.getMessage()));
        bitExtBuilder.setResult(retBuilder);
        logger.warn(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      } catch (RuntimeException e) {
        retBuilder.setResult(false).setCode(response_code.CONTRACT_EXE_ERROR)
            .setMessage(ByteString.copyFromUtf8(e.getClass() + " : " + e.getMessage()));
        bitExtBuilder.setResult(retBuilder);
        logger.warn("When run constant call in VM, have Runtime Exception: " + e.getMessage());
      } catch (Exception e) {
        retBuilder.setResult(false).setCode(response_code.OTHER_ERROR)
            .setMessage(ByteString.copyFromUtf8(e.getClass() + " : " + e.getMessage()));
        bitExtBuilder.setResult(retBuilder);
        logger.warn(UNKNOWN_EXCEPTION_CAUGHT + e.getMessage(), e);
      } finally {
        responseObserver.onNext(bitExtBuilder.build());
        responseObserver.onCompleted();
      }
    }

    public void getPaginatedAssetIssueList(PaginatedMessage request,
        StreamObserver<AssetIssueList> responseObserver) {
      responseObserver.onNext(wallet.getAssetIssueList(request.getOffset(), request.getLimit()));
      responseObserver.onCompleted();
    }

    @Override
    public void getContract(BytesMessage request,
        StreamObserver<SmartContract> responseObserver) {
      SmartContract contract = wallet.getContract(request);
      responseObserver.onNext(contract);
      responseObserver.onCompleted();
    }

    @Override
    public void getContractInfo(BytesMessage request,
        StreamObserver<SmartContractDataWrapper> responseObserver) {
      SmartContractDataWrapper contract = wallet.getContractInfo(request);
      responseObserver.onNext(contract);
      responseObserver.onCompleted();
    }

    public void listWitnesses(EmptyMessage request,
        StreamObserver<WitnessList> responseObserver) {
      responseObserver.onNext(wallet.getWitnessList());
      responseObserver.onCompleted();
    }

    @Override
    public void listProposals(EmptyMessage request,
        StreamObserver<ProposalList> responseObserver) {
      responseObserver.onNext(wallet.getProposalList());
      responseObserver.onCompleted();
    }


    @Override
    public void getDelegatedResource(DelegatedResourceMessage request,
        StreamObserver<DelegatedResourceList> responseObserver) {
      responseObserver
          .onNext(wallet.getDelegatedResource(request.getFromAddress(), request.getToAddress()));
      responseObserver.onCompleted();
    }

    @Override
    public void getDelegatedResourceV2(DelegatedResourceMessage request,
        StreamObserver<DelegatedResourceList> responseObserver) {
      try {
        responseObserver.onNext(wallet.getDelegatedResourceV2(
                request.getFromAddress(), request.getToAddress())
        );
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getDelegatedResourceAccountIndex(BytesMessage request,
        StreamObserver<org.bit.protos.Protocol.DelegatedResourceAccountIndex> responseObserver) {
      try {
        responseObserver
          .onNext(wallet.getDelegatedResourceAccountIndex(request.getValue()));
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getDelegatedResourceAccountIndexV2(BytesMessage request,
        StreamObserver<org.bit.protos.Protocol.DelegatedResourceAccountIndex> responseObserver) {
      try {
        responseObserver
                .onNext(wallet.getDelegatedResourceAccountIndexV2(request.getValue()));
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getCanDelegatedMaxSize(GrpcAPI.CanDelegatedMaxSizeRequestMessage request,
        StreamObserver<GrpcAPI.CanDelegatedMaxSizeResponseMessage> responseObserver) {
      try {
        responseObserver.onNext(wallet.getCanDelegatedMaxSize(
                        request.getOwnerAddress(), request.getType()));
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }

      responseObserver.onCompleted();
    }

    @Override
    public void getAvailableUnfreezeCount(GrpcAPI.GetAvailableUnfreezeCountRequestMessage request,
         StreamObserver<GrpcAPI.GetAvailableUnfreezeCountResponseMessage> responseObserver) {
      try {
        responseObserver.onNext(wallet.getAvailableUnfreezeCount(
                request.getOwnerAddress()));
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }

      responseObserver.onCompleted();
    }

    @Override
    public void getCanWithdrawUnfreezeAmount(CanWithdrawUnfreezeAmountRequestMessage request,
        StreamObserver<GrpcAPI.CanWithdrawUnfreezeAmountResponseMessage> responseObserver) {
      try {
        responseObserver
                .onNext(wallet.getCanWithdrawUnfreezeAmount(
                        request.getOwnerAddress(), request.getTimestamp()
        ));
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getBandwidthPrices(EmptyMessage request,
        StreamObserver<PricesResponseMessage> responseObserver) {
      try {
        responseObserver.onNext(wallet.getBandwidthPrices());
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getEnergyPrices(EmptyMessage request,
        StreamObserver<PricesResponseMessage> responseObserver) {
      try {
        responseObserver.onNext(wallet.getEnergyPrices());
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getMemoFee(EmptyMessage request,
        StreamObserver<PricesResponseMessage> responseObserver) {
      try {
        responseObserver.onNext(wallet.getMemoFeePrices());
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getPaginatedProposalList(PaginatedMessage request,
        StreamObserver<ProposalList> responseObserver) {
      responseObserver
          .onNext(wallet.getPaginatedProposalList(request.getOffset(), request.getLimit()));
      responseObserver.onCompleted();

    }

    @Override
    public void getPaginatedExchangeList(PaginatedMessage request,
        StreamObserver<ExchangeList> responseObserver) {
      responseObserver
          .onNext(wallet.getPaginatedExchangeList(request.getOffset(), request.getLimit()));
      responseObserver.onCompleted();

    }

    @Override
    public void listExchanges(EmptyMessage request,
        StreamObserver<ExchangeList> responseObserver) {
      responseObserver.onNext(wallet.getExchangeList());
      responseObserver.onCompleted();
    }

    @Override
    public void getChainParameters(EmptyMessage request,
        StreamObserver<Protocol.ChainParameters> responseObserver) {
      responseObserver.onNext(wallet.getChainParameters());
      responseObserver.onCompleted();
    }

    @Override
    public void getTransactionInfoById(BytesMessage request,
        StreamObserver<TransactionInfo> responseObserver) {
      ByteString id = request.getValue();
      if (null != id) {
        TransactionInfo reply = wallet.getTransactionInfoById(id);

        responseObserver.onNext(reply);
      } else {
        responseObserver.onNext(null);
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getNodeInfo(EmptyMessage request, StreamObserver<NodeInfo> responseObserver) {
      try {
        responseObserver.onNext(nodeInfoService.getNodeInfo().transferToProtoEntity());
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void accountPermissionUpdate(AccountPermissionUpdateContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.AccountPermissionUpdateContract,
          responseObserver);
    }

    @Override
    public void getMerkleTreeVoucherInfo(OutputPointInfo request,
        StreamObserver<IncrementalMerkleVoucherInfo> responseObserver) {

      try {
        checkSupportShieldedTransaction();

        IncrementalMerkleVoucherInfo witnessInfo = wallet
            .getMerkleTreeVoucherInfo(request);
        responseObserver.onNext(witnessInfo);
      } catch (Exception ex) {
        responseObserver.onError(getRunTimeException(ex));
        return;
      }

      responseObserver.onCompleted();
    }

    @Override
    public void createShieldedTransaction(PrivateParameters request,
        StreamObserver<TransactionExtention> responseObserver) {

      TransactionExtention.Builder bitExtBuilder = TransactionExtention.newBuilder();
      Return.Builder retBuilder = Return.newBuilder();

      try {
        checkSupportShieldedTransaction();

        TransactionCapsule bit = wallet.createShieldedTransaction(request);
        bitExtBuilder.setTransaction(bit.getInstance());
        bitExtBuilder.setTxid(bit.getTransactionId().getByteString());
        retBuilder.setResult(true).setCode(response_code.SUCCESS);
      } catch (ContractValidateException | ZksnarkException e) {
        retBuilder.setResult(false).setCode(response_code.CONTRACT_VALIDATE_ERROR)
            .setMessage(ByteString
                .copyFromUtf8(Wallet.CONTRACT_VALIDATE_ERROR + e.getMessage()));
        logger.debug(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      } catch (Exception e) {
        retBuilder.setResult(false).setCode(response_code.OTHER_ERROR)
            .setMessage(ByteString.copyFromUtf8(e.getClass() + " : " + e.getMessage()));
        logger.info("createShieldedTransaction exception caught: " + e.getMessage());
      }

      bitExtBuilder.setResult(retBuilder);
      responseObserver.onNext(bitExtBuilder.build());
      responseObserver.onCompleted();

    }

    @Override
    public void createShieldedTransactionWithoutSpendAuthSig(PrivateParametersWithoutAsk request,
        StreamObserver<TransactionExtention> responseObserver) {

      TransactionExtention.Builder bitExtBuilder = TransactionExtention.newBuilder();
      Return.Builder retBuilder = Return.newBuilder();

      try {
        checkSupportShieldedTransaction();

        TransactionCapsule bit = wallet.createShieldedTransactionWithoutSpendAuthSig(request);
        bitExtBuilder.setTransaction(bit.getInstance());
        bitExtBuilder.setTxid(bit.getTransactionId().getByteString());
        retBuilder.setResult(true).setCode(response_code.SUCCESS);
      } catch (ContractValidateException | ZksnarkException e) {
        retBuilder.setResult(false).setCode(response_code.CONTRACT_VALIDATE_ERROR)
            .setMessage(ByteString
                .copyFromUtf8(Wallet.CONTRACT_VALIDATE_ERROR + e.getMessage()));
        logger.debug(CONTRACT_VALIDATE_EXCEPTION, e.getMessage());
      } catch (Exception e) {
        retBuilder.setResult(false).setCode(response_code.OTHER_ERROR)
            .setMessage(ByteString.copyFromUtf8(e.getClass() + " : " + e.getMessage()));
        logger.info(
            "createShieldedTransactionWithoutSpendAuthSig exception caught: " + e.getMessage());
      }

      bitExtBuilder.setResult(retBuilder);
      responseObserver.onNext(bitExtBuilder.build());
      responseObserver.onCompleted();

    }

    @Override
    public void getNewShieldedAddress(EmptyMessage request,
        StreamObserver<ShieldedAddressInfo> responseObserver) {

      try {
        checkSupportShieldedBRC20Transaction();

        responseObserver.onNext(wallet.getNewShieldedAddress());
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
        return;
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getSpendingKey(EmptyMessage request,
        StreamObserver<BytesMessage> responseObserver) {
      try {
        checkSupportShieldedBRC20Transaction();

        responseObserver.onNext(wallet.getSpendingKey());
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
        return;
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getRcm(EmptyMessage request,
        StreamObserver<BytesMessage> responseObserver) {
      try {
        checkSupportShieldedBRC20Transaction();

        responseObserver.onNext(wallet.getRcm());
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
        return;
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getExpandedSpendingKey(BytesMessage request,
        StreamObserver<ExpandedSpendingKeyMessage> responseObserver) {
      ByteString spendingKey = request.getValue();

      try {
        checkSupportShieldedBRC20Transaction();

        ExpandedSpendingKeyMessage response = wallet.getExpandedSpendingKey(spendingKey);
        responseObserver.onNext(response);
      } catch (BadItemException | ZksnarkException e) {
        responseObserver.onError(getRunTimeException(e));
        return;
      }

      responseObserver.onCompleted();
    }

    @Override
    public void getAkFromAsk(BytesMessage request, StreamObserver<BytesMessage> responseObserver) {
      ByteString ak = request.getValue();

      try {
        checkSupportShieldedBRC20Transaction();

        responseObserver.onNext(wallet.getAkFromAsk(ak));
      } catch (BadItemException | ZksnarkException e) {
        responseObserver.onError(getRunTimeException(e));
        return;
      }

      responseObserver.onCompleted();
    }

    @Override
    public void getNkFromNsk(BytesMessage request, StreamObserver<BytesMessage> responseObserver) {
      ByteString nk = request.getValue();

      try {
        checkSupportShieldedBRC20Transaction();

        responseObserver.onNext(wallet.getNkFromNsk(nk));
      } catch (BadItemException | ZksnarkException e) {
        responseObserver.onError(getRunTimeException(e));
        return;
      }

      responseObserver.onCompleted();
    }

    @Override
    public void getIncomingViewingKey(ViewingKeyMessage request,
        StreamObserver<IncomingViewingKeyMessage> responseObserver) {
      ByteString ak = request.getAk();
      ByteString nk = request.getNk();

      try {
        checkSupportShieldedBRC20Transaction();

        responseObserver.onNext(wallet.getIncomingViewingKey(ak.toByteArray(), nk.toByteArray()));
      } catch (ZksnarkException e) {
        responseObserver.onError(getRunTimeException(e));
        return;
      }

      responseObserver.onCompleted();
    }

    @Override
    public void getDiversifier(EmptyMessage request,
        StreamObserver<DiversifierMessage> responseObserver) {
      try {
        checkSupportShieldedBRC20Transaction();

        DiversifierMessage d = wallet.getDiversifier();
        responseObserver.onNext(d);
      } catch (ZksnarkException e) {
        responseObserver.onError(getRunTimeException(e));
        return;
      }
      responseObserver.onCompleted();
    }


    @Override
    public void getZenPaymentAddress(IncomingViewingKeyDiversifierMessage request,
        StreamObserver<PaymentAddressMessage> responseObserver) {
      IncomingViewingKeyMessage ivk = request.getIvk();
      DiversifierMessage d = request.getD();

      try {
        checkSupportShieldedBRC20Transaction();

        PaymentAddressMessage saplingPaymentAddressMessage =
            wallet.getPaymentAddress(new IncomingViewingKey(ivk.getIvk().toByteArray()),
                new DiversifierT(d.getD().toByteArray()));

        responseObserver.onNext(saplingPaymentAddressMessage);
      } catch (BadItemException | ZksnarkException e) {
        responseObserver.onError(getRunTimeException(e));
        return;
      }
      responseObserver.onCompleted();

    }

    @Override
    public void scanNoteByIvk(GrpcAPI.IvkDecryptParameters request,
        StreamObserver<GrpcAPI.DecryptNotes> responseObserver) {
      long startNum = request.getStartBlockIndex();
      long endNum = request.getEndBlockIndex();

      try {
        checkSupportShieldedTransaction();

        DecryptNotes decryptNotes = wallet
            .scanNoteByIvk(startNum, endNum, request.getIvk().toByteArray());
        responseObserver.onNext(decryptNotes);
      } catch (BadItemException | ZksnarkException e) {
        responseObserver.onError(getRunTimeException(e));
        return;
      }
      responseObserver.onCompleted();

    }

    @Override
    public void scanAndMarkNoteByIvk(GrpcAPI.IvkDecryptAndMarkParameters request,
        StreamObserver<GrpcAPI.DecryptNotesMarked> responseObserver) {
      long startNum = request.getStartBlockIndex();
      long endNum = request.getEndBlockIndex();

      try {
        checkSupportShieldedTransaction();

        DecryptNotesMarked decryptNotes = wallet.scanAndMarkNoteByIvk(startNum, endNum,
            request.getIvk().toByteArray(),
            request.getAk().toByteArray(),
            request.getNk().toByteArray());
        responseObserver.onNext(decryptNotes);
      } catch (BadItemException | ZksnarkException | InvalidProtocolBufferException
          | ItemNotFoundException e) {
        responseObserver.onError(getRunTimeException(e));
        return;
      }
      responseObserver.onCompleted();
    }

    @Override
    public void scanNoteByOvk(GrpcAPI.OvkDecryptParameters request,
        StreamObserver<GrpcAPI.DecryptNotes> responseObserver) {
      long startNum = request.getStartBlockIndex();
      long endNum = request.getEndBlockIndex();

      try {
        checkSupportShieldedTransaction();

        DecryptNotes decryptNotes = wallet
            .scanNoteByOvk(startNum, endNum, request.getOvk().toByteArray());
        responseObserver.onNext(decryptNotes);
      } catch (BadItemException | ZksnarkException e) {
        responseObserver.onError(getRunTimeException(e));
        return;
      }
      responseObserver.onCompleted();
    }

    @Override
    public void isSpend(NoteParameters request, StreamObserver<SpendResult> responseObserver) {
      try {
        checkSupportShieldedTransaction();

        responseObserver.onNext(wallet.isSpend(request));
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
        return;
      }
      responseObserver.onCompleted();
    }

    @Override
    public void createShieldNullifier(GrpcAPI.NfParameters request,
        StreamObserver<GrpcAPI.BytesMessage> responseObserver) {
      try {
        checkSupportShieldedTransaction();

        BytesMessage nf = wallet
            .createShieldNullifier(request);
        responseObserver.onNext(nf);
      } catch (ZksnarkException e) {
        responseObserver.onError(getRunTimeException(e));
        return;
      }
      responseObserver.onCompleted();
    }

    @Override
    public void createSpendAuthSig(SpendAuthSigParameters request,
        StreamObserver<GrpcAPI.BytesMessage> responseObserver) {
      try {
        checkSupportShieldedBRC20Transaction();

        BytesMessage spendAuthSig = wallet.createSpendAuthSig(request);
        responseObserver.onNext(spendAuthSig);
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
        return;
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getShieldTransactionHash(Transaction request,
        StreamObserver<GrpcAPI.BytesMessage> responseObserver) {
      try {
        checkSupportShieldedTransaction();

        BytesMessage transactionHash = wallet.getShieldTransactionHash(request);
        responseObserver.onNext(transactionHash);
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
        return;
      }
      responseObserver.onCompleted();
    }

    @Override
    public void createShieldedContractParameters(
        PrivateShieldedBRC20Parameters request,
        StreamObserver<org.bit.api.GrpcAPI.ShieldedBRC20Parameters> responseObserver) {
      try {
        checkSupportShieldedBRC20Transaction();

        ShieldedBRC20Parameters shieldedBRC20Parameters = wallet
            .createShieldedContractParameters(request);
        responseObserver.onNext(shieldedBRC20Parameters);
      } catch (ZksnarkException | ContractValidateException | ContractExeException e) {
        responseObserver.onError(getRunTimeException(e));
        logger.info("createShieldedContractParameters: {}", e.getMessage());
        return;
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
        logger.error("createShieldedContractParameters: ", e);
        return;
      }
      responseObserver.onCompleted();
    }

    @Override
    public void createShieldedContractParametersWithoutAsk(
        PrivateShieldedBRC20ParametersWithoutAsk request,
        StreamObserver<org.bit.api.GrpcAPI.ShieldedBRC20Parameters> responseObserver) {
      try {
        checkSupportShieldedBRC20Transaction();

        ShieldedBRC20Parameters shieldedBRC20Parameters = wallet
            .createShieldedContractParametersWithoutAsk(request);
        responseObserver.onNext(shieldedBRC20Parameters);
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
        logger
            .info("createShieldedContractParametersWithoutAsk exception caught: " + e.getMessage());
        return;
      }
      responseObserver.onCompleted();
    }

    @Override
    public void scanShieldedBRC20NotesByIvk(
        IvkDecryptBRC20Parameters request,
        StreamObserver<org.bit.api.GrpcAPI.DecryptNotesBRC20> responseObserver) {
      long startNum = request.getStartBlockIndex();
      long endNum = request.getEndBlockIndex();
      try {
        checkSupportShieldedBRC20Transaction();

        DecryptNotesBRC20 decryptNotes = wallet.scanShieldedBRC20NotesByIvk(startNum, endNum,
            request.getShieldedBRC20ContractAddress().toByteArray(),
            request.getIvk().toByteArray(),
            request.getAk().toByteArray(),
            request.getNk().toByteArray(),
            request.getEventsList());
        responseObserver.onNext(decryptNotes);
      } catch (BadItemException | ZksnarkException e) {
        responseObserver.onError(getRunTimeException(e));
        logger.info("scanShieldedBRC20NotesByIvk: {}", e.getMessage());
        return;
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
        logger.error("scanShieldedBRC20NotesByIvk:", e);
        return;
      }
      responseObserver.onCompleted();
    }

    @Override
    public void scanShieldedBRC20NotesByOvk(
        OvkDecryptBRC20Parameters request,
        StreamObserver<org.bit.api.GrpcAPI.DecryptNotesBRC20> responseObserver) {
      long startNum = request.getStartBlockIndex();
      long endNum = request.getEndBlockIndex();
      try {
        checkSupportShieldedBRC20Transaction();

        DecryptNotesBRC20 decryptNotes = wallet.scanShieldedBRC20NotesByOvk(startNum, endNum,
            request.getOvk().toByteArray(),
            request.getShieldedBRC20ContractAddress().toByteArray(),
            request.getEventsList());
        responseObserver.onNext(decryptNotes);
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
        logger.info("scanShieldedBRC20NotesByOvk exception caught: " + e.getMessage());
        return;
      }
      responseObserver.onCompleted();
    }

    @Override
    public void isShieldedBRC20ContractNoteSpent(NfBRC20Parameters request,
        StreamObserver<GrpcAPI.NullifierResult> responseObserver) {
      try {
        checkSupportShieldedBRC20Transaction();

        GrpcAPI.NullifierResult nf = wallet
            .isShieldedBRC20ContractNoteSpent(request);
        responseObserver.onNext(nf);
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
        return;
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getTriggerInputForShieldedBRC20Contract(
        ShieldedBRC20TriggerContractParameters request,
        StreamObserver<org.bit.api.GrpcAPI.BytesMessage> responseObserver) {
      try {
        checkSupportShieldedBRC20Transaction();

        responseObserver.onNext(wallet.getTriggerInputForShieldedBRC20Contract(request));
      } catch (Exception e) {
        responseObserver.onError(e);
        return;
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getRewardInfo(BytesMessage request,
        StreamObserver<StringMessage> responseObserver) {
      getRewardInfoCommon(request, responseObserver);
    }

    @Override
    public void getBrokerageInfo(BytesMessage request,
        StreamObserver<NumberMessage> responseObserver) {
      getBrokerageInfoCommon(request, responseObserver);
    }

    @Override
    public void getBurnBit(EmptyMessage request, StreamObserver<NumberMessage> responseObserver) {
      getBurnBitCommon(request, responseObserver);
    }

    @Override
    public void updateBrokerage(UpdateBrokerageContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.UpdateBrokerageContract,
          responseObserver);
    }

    @Override
    public void createCommonTransaction(Transaction request,
        StreamObserver<TransactionExtention> responseObserver) {
      Transaction.Contract contract = request.getRawData().getContract(0);
      createTransactionExtention(contract.getParameter(), contract.getType(),
          responseObserver);
    }

    @Override
    public void getTransactionInfoByBlockNum(NumberMessage request,
        StreamObserver<TransactionInfoList> responseObserver) {
      try {
        responseObserver.onNext(wallet.getTransactionInfoByBlockNum(request.getNum()));
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }

      responseObserver.onCompleted();
    }

    @Override
    public void marketSellAsset(MarketSellAssetContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.MarketSellAssetContract,
          responseObserver);
    }

    @Override
    public void marketCancelOrder(MarketCancelOrderContract request,
        StreamObserver<TransactionExtention> responseObserver) {
      createTransactionExtention(request, ContractType.MarketCancelOrderContract, responseObserver);
    }

    @Override
    public void getMarketOrderByAccount(BytesMessage request,
        StreamObserver<MarketOrderList> responseObserver) {
      try {
        ByteString address = request.getValue();

        MarketOrderList marketOrderList = wallet
            .getMarketOrderByAccount(address);
        responseObserver.onNext(marketOrderList);
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getMarketOrderById(BytesMessage request,
        StreamObserver<MarketOrder> responseObserver) {
      try {
        ByteString address = request.getValue();

        MarketOrder marketOrder = wallet
            .getMarketOrderById(address);
        responseObserver.onNext(marketOrder);
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getMarketPriceByPair(MarketOrderPair request,
        StreamObserver<MarketPriceList> responseObserver) {
      try {
        MarketPriceList marketPriceList = wallet
            .getMarketPriceByPair(request.getSellTokenId().toByteArray(),
                request.getBuyTokenId().toByteArray());
        responseObserver.onNext(marketPriceList);
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getMarketOrderListByPair(org.bit.protos.Protocol.MarketOrderPair request,
        StreamObserver<MarketOrderList> responseObserver) {
      try {
        MarketOrderList orderPairList = wallet
            .getMarketOrderListByPair(request.getSellTokenId().toByteArray(),
                request.getBuyTokenId().toByteArray());
        responseObserver.onNext(orderPairList);
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getMarketPairList(EmptyMessage request,
        StreamObserver<MarketOrderPairList> responseObserver) {
      try {
        MarketOrderPairList pairList = wallet.getMarketPairList();
        responseObserver.onNext(pairList);
      } catch (Exception e) {
        responseObserver.onError(getRunTimeException(e));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void getTransactionFromPending(BytesMessage request,
        StreamObserver<Transaction> responseObserver) {
      getTransactionFromPendingCommon(request, responseObserver);
    }

    @Override
    public void getTransactionListFromPending(EmptyMessage request,
        StreamObserver<TransactionIdList> responseObserver) {
      getTransactionListFromPendingCommon(request, responseObserver);
    }

    @Override
    public void getPendingSize(EmptyMessage request,
        StreamObserver<NumberMessage> responseObserver) {
      getPendingSizeCommon(request, responseObserver);
    }


    @Override
    public void getBlock(GrpcAPI.BlockReq  request,
        StreamObserver<BlockExtention> responseObserver) {
      getBlockCommon(request, responseObserver);
    }
  }

  public class MonitorApi extends MonitorGrpc.MonitorImplBase {

    @Override
    public void getStatsInfo(EmptyMessage request,
        StreamObserver<Protocol.MetricsInfo> responseObserver) {
      responseObserver.onNext(metricsApiService.getMetricProtoInfo());
      responseObserver.onCompleted();
    }
  }

  public void getRewardInfoCommon(BytesMessage request,
      StreamObserver<StringMessage> responseObserver) {
    try {
      BigInteger value = dbManager.getMortgageService().queryReward(request.getValue().toByteArray());
      StringMessage.Builder builder = StringMessage.newBuilder();
      builder.setValue(value.toString());
      responseObserver.onNext(builder.build());
    } catch (Exception e) {
      responseObserver.onError(e);
    }
    responseObserver.onCompleted();
  }

  public void getBurnBitCommon(EmptyMessage request,
      StreamObserver<NumberMessage> responseObserver) {
    try {
      long value = dbManager.getDynamicPropertiesStore().getBurnBitAmount();
      NumberMessage.Builder builder = NumberMessage.newBuilder();
      builder.setNum(value);
      responseObserver.onNext(builder.build());
    } catch (Exception e) {
      responseObserver.onError(e);
    }
    responseObserver.onCompleted();
  }

  public void getBrokerageInfoCommon(BytesMessage request,
      StreamObserver<NumberMessage> responseObserver) {
    try {
      long cycle = dbManager.getDynamicPropertiesStore().getCurrentCycleNumber();
      long value = dbManager.getDelegationStore()
          .getBrokerage(cycle, request.getValue().toByteArray());
      NumberMessage.Builder builder = NumberMessage.newBuilder();
      builder.setNum(value);
      responseObserver.onNext(builder.build());
    } catch (Exception e) {
      responseObserver.onError(e);
    }
    responseObserver.onCompleted();
  }

  public void getTransactionCountByBlockNumCommon(NumberMessage request,
      StreamObserver<NumberMessage> responseObserver) {
    NumberMessage.Builder builder = NumberMessage.newBuilder();
    try {
      Block block = chainBaseManager.getBlockByNum(request.getNum()).getInstance();
      builder.setNum(block.getTransactionsCount());
    } catch (StoreException e) {
      logger.error(e.getMessage());
      builder.setNum(-1);
    }
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  public void getTransactionFromPendingCommon(BytesMessage request,
      StreamObserver<Transaction> responseObserver) {
    try {
      String txId = ByteArray.toHexString(request.getValue().toByteArray());
      TransactionCapsule transactionCapsule = dbManager.getTxFromPending(txId);
      responseObserver.onNext(transactionCapsule == null ? null : transactionCapsule.getInstance());
    } catch (Exception e) {
      responseObserver.onError(e);
    }
    responseObserver.onCompleted();
  }

  public void getTransactionListFromPendingCommon(EmptyMessage request,
      StreamObserver<TransactionIdList> responseObserver) {
    try {
      TransactionIdList.Builder builder = TransactionIdList.newBuilder();
      builder.addAllTxId(dbManager.getTxListFromPending());
      responseObserver.onNext(builder.build());
    } catch (Exception e) {
      responseObserver.onError(e);
    }
    responseObserver.onCompleted();
  }

  public void getPendingSizeCommon(EmptyMessage request,
      StreamObserver<NumberMessage> responseObserver) {
    try {
      NumberMessage.Builder builder = NumberMessage.newBuilder();
      builder.setNum(dbManager.getPendingSize());
      responseObserver.onNext(builder.build());
    } catch (Exception e) {
      responseObserver.onError(e);
    }
    responseObserver.onCompleted();
  }

  public void getBlockCommon(GrpcAPI.BlockReq request,
      StreamObserver<BlockExtention> responseObserver) {
    try {
      responseObserver.onNext(block2Extention(wallet.getBlock(request)));
    } catch (Exception e) {
      if (e instanceof IllegalArgumentException) {
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage())
            .withCause(e).asRuntimeException());
      } else {
        responseObserver.onError(getRunTimeException(e));
      }
    }
    responseObserver.onCompleted();
  }

}
