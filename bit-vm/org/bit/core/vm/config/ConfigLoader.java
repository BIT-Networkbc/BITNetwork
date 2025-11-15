package org.bit.core.vm.config;

import static org.bit.core.capsule.ReceiptCapsule.checkForEnergyLimit;

import lombok.extern.slf4j.Slf4j;
import org.bit.common.parameter.CommonParameter;
import org.bit.core.store.DynamicPropertiesStore;
import org.bit.core.store.StoreFactory;

@Slf4j(topic = "VMConfigLoader")
public class ConfigLoader {

  //only for unit test
  public static boolean disable = false;

  public static void load(StoreFactory storeFactory) {
    if (!disable) {
      DynamicPropertiesStore ds = storeFactory.getChainBaseManager().getDynamicPropertiesStore();
      VMConfig.setVmTrace(CommonParameter.getInstance().isVmTrace());
      if (ds != null) {
        VMConfig.initVmHardFork(checkForEnergyLimit(ds));
        VMConfig.initAllowMultiSign(ds.getAllowMultiSign());
        VMConfig.initAllowTvmTransferBrc10(ds.getAllowTvmTransferBrc10());
        VMConfig.initAllowTvmConstantinople(ds.getAllowTvmConstantinople());
        VMConfig.initAllowTvmSolidity059(ds.getAllowTvmSolidity059());
        VMConfig.initAllowShieldedBRC20Transaction(ds.getAllowShieldedBRC20Transaction());
        VMConfig.initAllowTvmIstanbul(ds.getAllowTvmIstanbul());
        VMConfig.initAllowTvmFreeze(ds.getAllowTvmFreeze());
        VMConfig.initAllowTvmVote(ds.getAllowTvmVote());
        VMConfig.initAllowTvmLondon(ds.getAllowTvmLondon());
        VMConfig.initAllowTvmCompatibleEvm(ds.getAllowTvmCompatibleEvm());
        VMConfig.initAllowHigherLimitForMaxCpuTimeOfOneTx(
            ds.getAllowHigherLimitForMaxCpuTimeOfOneTx());
        VMConfig.initAllowTvmFreezeV2(ds.supportUnfreezeDelay() ? 1 : 0);
        VMConfig.initAllowOptimizedReturnValueOfChainId(
            ds.getAllowOptimizedReturnValueOfChainId());
        VMConfig.initAllowDynamicEnergy(ds.getAllowDynamicEnergy());
        VMConfig.initDynamicEnergyThreshold(ds.getDynamicEnergyThreshold());
        VMConfig.initDynamicEnergyIncreaseFactor(ds.getDynamicEnergyIncreaseFactor());
        VMConfig.initDynamicEnergyMaxFactor(ds.getDynamicEnergyMaxFactor());
        VMConfig.initAllowTvmShangHai(ds.getAllowTvmShangHai());
      }
    }
  }
}
