package org.bit.core.db;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.bit.core.capsule.BytesCapsule;
import org.bit.core.db2.common.TxCacheDB;
import org.bit.core.store.DynamicPropertiesStore;

@Slf4j
@Component
public class TransactionCache extends BitStoreWithRevoking<BytesCapsule> {

  @Autowired
  public TransactionCache(@Value("trans-cache") String dbName,
                          @Autowired RecentTransactionStore recentTransactionStore,
                          @Autowired DynamicPropertiesStore dynamicPropertiesStore) {
    super(new TxCacheDB(dbName, recentTransactionStore, dynamicPropertiesStore));
  }

  public void initCache() {
    ((TxCacheDB) getDb()).init();
  }
}
