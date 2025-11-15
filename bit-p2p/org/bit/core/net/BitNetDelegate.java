package org.bit.core.net;

import static org.bit.core.config.Parameter.ChainConstant.BLOCK_PRODUCED_INTERVAL;
import static org.bit.core.exception.BadBlockException.TypeEnum.CALC_MERKLE_ROOT_FAILED;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.prometheus.client.Histogram;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.bit.common.backup.socket.BackupServer;
import org.bit.common.overlay.message.Message;
import org.bit.common.prometheus.MetricKeys;
import org.bit.common.prometheus.MetricLabels;
import org.bit.common.prometheus.Metrics;
import org.bit.common.utils.Sha256Hash;
import org.bit.core.ChainBaseManager;
import org.bit.core.capsule.BlockCapsule;
import org.bit.core.capsule.BlockCapsule.BlockId;
import org.bit.core.capsule.PbftSignCapsule;
import org.bit.core.capsule.TransactionCapsule;
import org.bit.core.config.args.Args;
import org.bit.core.db.Manager;
import org.bit.core.exception.AccountResourceInsufficientException;
import org.bit.core.exception.BadBlockException;
import org.bit.core.exception.BadItemException;
import org.bit.core.exception.BadNumberBlockException;
import org.bit.core.exception.ContractExeException;
import org.bit.core.exception.ContractSizeNotEqualToOneException;
import org.bit.core.exception.ContractValidateException;
import org.bit.core.exception.DupTransactionException;
import org.bit.core.exception.EventBloomException;
import org.bit.core.exception.ItemNotFoundException;
import org.bit.core.exception.NonCommonBlockException;
import org.bit.core.exception.P2pException;
import org.bit.core.exception.P2pException.TypeEnum;
import org.bit.core.exception.ReceiptCheckErrException;
import org.bit.core.exception.StoreException;
import org.bit.core.exception.TaposException;
import org.bit.core.exception.TooBigTransactionException;
import org.bit.core.exception.TooBigTransactionResultException;
import org.bit.core.exception.TransactionExpirationException;
import org.bit.core.exception.UnLinkedBlockException;
import org.bit.core.exception.VMIllegalException;
import org.bit.core.exception.ValidateScheduleException;
import org.bit.core.exception.ValidateSignatureException;
import org.bit.core.exception.ZksnarkException;
import org.bit.core.metrics.MetricsService;
import org.bit.core.net.message.MessageTypes;
import org.bit.core.net.message.adv.BlockMessage;
import org.bit.core.net.message.adv.TransactionMessage;
import org.bit.core.net.peer.PeerConnection;
import org.bit.core.store.WitnessScheduleStore;
import org.bit.protos.Protocol.Inventory.InventoryType;

@Slf4j(topic = "net")
@Component
public class BitNetDelegate {

  @Autowired
  private Manager dbManager;

  @Autowired
  private ChainBaseManager chainBaseManager;

  @Autowired
  private WitnessScheduleStore witnessScheduleStore;

  @Getter
  private Object blockLock = new Object();

  @Autowired
  private BackupServer backupServer;

  @Autowired
  private MetricsService metricsService;

  private volatile boolean backupServerStartFlag;

  private int blockIdCacheSize = 100;

  private long timeout = 1000;

  @Getter // for test
  private volatile boolean  hitDown = false;

  private Thread hitThread;

  @Setter
  private volatile boolean exit = true;

  private int maxUnsolidifiedBlocks = Args.getInstance().getMaxUnsolidifiedBlocks();

  private boolean unsolidifiedBlockCheck
      = Args.getInstance().isUnsolidifiedBlockCheck();

  private Cache<BlockId, Long> freshBlockId = CacheBuilder.newBuilder()
          .maximumSize(blockIdCacheSize).expireAfterWrite(1, TimeUnit.HOURS)
          .recordStats().build();

  @PostConstruct
  public void init() {
    hitThread =  new Thread(() -> {
      LockSupport.park();
      // to Guarantee Some other thread invokes unpark with the current thread as the target
      if (hitDown && exit) {
        System.exit(0);
      }
    });
    hitThread.setName("hit-thread");
    hitThread.start();
  }

  @PreDestroy
  public void close() {
    try {
      hitThread.interrupt();
      // help GC
      hitThread = null;
    } catch (Exception e) {
      logger.warn("hitThread interrupt error", e);
    }
  }

  public Collection<PeerConnection> getActivePeer() {
    return BitNetService.getPeers();
  }

  public long getSyncBeginNumber() {
    return dbManager.getSyncBeginNumber();
  }

  public long getBlockTime(BlockId id) throws P2pException {
    try {
      return chainBaseManager.getBlockById(id).getTimeStamp();
    } catch (BadItemException | ItemNotFoundException e) {
      throw new P2pException(TypeEnum.DB_ITEM_NOT_FOUND, id.getString());
    }
  }

  public BlockId getHeadBlockId() {
    return chainBaseManager.getHeadBlockId();
  }

  public BlockId getKhaosDbHeadBlockId() {
    return chainBaseManager.getKhaosDbHead().getBlockId();
  }

  public BlockId getSolidBlockId() {
    return chainBaseManager.getSolidBlockId();
  }

  public BlockId getGenesisBlockId() {
    return chainBaseManager.getGenesisBlockId();
  }

  public BlockId getBlockIdByNum(long num) throws P2pException {
    try {
      return chainBaseManager.getBlockIdByNum(num);
    } catch (ItemNotFoundException e) {
      throw new P2pException(TypeEnum.DB_ITEM_NOT_FOUND, "num: " + num);
    }
  }

  public BlockCapsule getGenesisBlock() {
    return chainBaseManager.getGenesisBlock();
  }

  public long getHeadBlockTimeStamp() {
    return chainBaseManager.getHeadBlockTimeStamp();
  }

  public boolean containBlock(BlockId id) {
    return chainBaseManager.containBlock(id);
  }

  public boolean containBlockInMainChain(BlockId id) {
    return chainBaseManager.containBlockInMainChain(id);
  }

  public List<BlockId> getBlockChainHashesOnFork(BlockId forkBlockHash) throws P2pException {
    try {
      return dbManager.getBlockChainHashesOnFork(forkBlockHash);
    } catch (NonCommonBlockException e) {
      throw new P2pException(TypeEnum.HARD_FORKED, forkBlockHash.getString());
    }
  }

  public boolean canChainRevoke(long num) {
    return num >= dbManager.getSyncBeginNumber();
  }

  public boolean contain(Sha256Hash hash, MessageTypes type) {
    if (type.equals(MessageTypes.BLOCK)) {
      return chainBaseManager.containBlock(hash);
    } else if (type.equals(MessageTypes.BIT)) {
      return dbManager.getTransactionStore().has(hash.getBytes());
    }
    return false;
  }

  public Message getData(Sha256Hash hash, InventoryType type) throws P2pException {
    try {
      switch (type) {
        case BLOCK:
          return new BlockMessage(chainBaseManager.getBlockById(hash));
        case BIT:
          TransactionCapsule tx = chainBaseManager.getTransactionStore().get(hash.getBytes());
          if (tx != null) {
            return new TransactionMessage(tx.getInstance());
          }
          throw new StoreException();
        default:
          throw new StoreException();
      }
    } catch (StoreException e) {
      throw new P2pException(TypeEnum.DB_ITEM_NOT_FOUND,
          "type: " + type + ", hash: " + hash.getByteString());
    }
  }

  public void processBlock(BlockCapsule block, boolean isSync) throws P2pException {
    if (!hitDown && dbManager.getLatestSolidityNumShutDown() > 0
        && dbManager.getLatestSolidityNumShutDown() == dbManager.getDynamicPropertiesStore()
        .getLatestBlockHeaderNumberFromDB()) {

      logger.info("Begin shutdown, currentBlockNum:{}, DbBlockNum:{}, solidifiedBlockNum:{}",
          dbManager.getDynamicPropertiesStore().getLatestBlockHeaderNumber(),
          dbManager.getDynamicPropertiesStore().getLatestBlockHeaderNumberFromDB(),
          dbManager.getDynamicPropertiesStore().getLatestSolidifiedBlockNum());
      hitDown = true;
      LockSupport.unpark(hitThread);
      return;
    }
    if (hitDown) {
      return;
    }
    BlockId blockId = block.getBlockId();
    synchronized (blockLock) {
      try {
        if (freshBlockId.getIfPresent(blockId) == null) {
          if (block.getNum() <= getHeadBlockId().getNum()) {
            logger.warn("Receive a fork block {} witness {}, head {}",
                block.getBlockId().getString(),
                Hex.toHexString(block.getWitnessAddress().toByteArray()),
                getHeadBlockId().getString());
          }
          if (!isSync) {
            //record metrics
            metricsService.applyBlock(block);
          }
          dbManager.getBlockedTimer().set(Metrics.histogramStartTimer(
              MetricKeys.Histogram.LOCK_ACQUIRE_LATENCY, MetricLabels.BLOCK));
          Histogram.Timer timer = Metrics.histogramStartTimer(
              MetricKeys.Histogram.BLOCK_PROCESS_LATENCY, String.valueOf(isSync));
          dbManager.pushBlock(block);
          Metrics.histogramObserve(timer);
          freshBlockId.put(blockId, System.currentTimeMillis());
          logger.info("Success process block {}", blockId.getString());
          if (!backupServerStartFlag
              && System.currentTimeMillis() - block.getTimeStamp() < BLOCK_PRODUCED_INTERVAL) {
            backupServerStartFlag = true;
            backupServer.initServer();
          }
        }
      } catch (ValidateSignatureException
          | ContractValidateException
          | ContractExeException
          | UnLinkedBlockException
          | ValidateScheduleException
          | AccountResourceInsufficientException
          | TaposException
          | TooBigTransactionException
          | TooBigTransactionResultException
          | DupTransactionException
          | TransactionExpirationException
          | BadNumberBlockException
          | BadBlockException
          | NonCommonBlockException
          | ReceiptCheckErrException
          | VMIllegalException
          | ZksnarkException
          | EventBloomException e) {
        metricsService.failProcessBlock(block.getNum(), e.getMessage());
        logger.error("Process block failed, {}, reason: {}", blockId.getString(), e.getMessage());
        if (e instanceof BadBlockException
                && ((BadBlockException) e).getType().equals(CALC_MERKLE_ROOT_FAILED)) {
          throw new P2pException(TypeEnum.BLOCK_MERKLE_ERROR, e);
        } else {
          throw new P2pException(TypeEnum.BAD_BLOCK, e);
        }
      }
    }
  }

  public void pushTransaction(TransactionCapsule bit) throws P2pException {
    try {
      bit.setTime(System.currentTimeMillis());
      dbManager.pushTransaction(bit);
    } catch (ContractSizeNotEqualToOneException
        | VMIllegalException e) {
      throw new P2pException(TypeEnum.BAD_BIT, e);
    } catch (ContractValidateException
        | ValidateSignatureException
        | ContractExeException
        | DupTransactionException
        | TaposException
        | TooBigTransactionException
        | TransactionExpirationException
        | ReceiptCheckErrException
        | TooBigTransactionResultException
        | AccountResourceInsufficientException e) {
      throw new P2pException(TypeEnum.BIT_EXE_FAILED, e);
    }
  }

  public void validSignature(BlockCapsule block) throws P2pException {
    boolean flag;
    try {
      flag = block.validateSignature(dbManager.getDynamicPropertiesStore(),
              dbManager.getAccountStore());
    } catch (Exception e) {
      throw new P2pException(TypeEnum.BLOCK_SIGN_ERROR, e);
    }
    if (!flag) {
      throw new P2pException(TypeEnum.BLOCK_SIGN_ERROR, "valid signature failed.");
    }
  }

  public boolean validBlock(BlockCapsule block) throws P2pException {
    long time = System.currentTimeMillis();
    if (block.getTimeStamp() - time > timeout) {
      throw new P2pException(TypeEnum.BAD_BLOCK,
              "time:" + time + ",block time:" + block.getTimeStamp());
    }
    validSignature(block);
    return witnessScheduleStore.getActiveWitnesses().contains(block.getWitnessAddress());
  }

  public PbftSignCapsule getBlockPbftCommitData(long blockNum) {
    return chainBaseManager.getPbftSignDataStore().getBlockSignData(blockNum);
  }

  public PbftSignCapsule getSRLPbftCommitData(long epoch) {
    return chainBaseManager.getPbftSignDataStore().getSrSignData(epoch);
  }

  public boolean allowPBFT() {
    return chainBaseManager.getDynamicPropertiesStore().allowPBFT();
  }

  public Object getForkLock() {
    return dbManager.getForkLock();
  }

  public long getNextMaintenanceTime() {
    return chainBaseManager.getDynamicPropertiesStore().getNextMaintenanceTime();
  }

  public long getMaintenanceTimeInterval() {
    return chainBaseManager.getDynamicPropertiesStore().getMaintenanceTimeInterval();
  }

  public boolean isBlockUnsolidified() {
    if (!unsolidifiedBlockCheck) {
      return false;
    }
    long headNum = chainBaseManager.getHeadBlockNum();
    long solidNum = chainBaseManager.getSolidBlockId().getNum();
    return headNum - solidNum >= maxUnsolidifiedBlocks;
  }

}
