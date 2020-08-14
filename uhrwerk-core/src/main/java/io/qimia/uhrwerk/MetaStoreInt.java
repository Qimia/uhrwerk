package io.qimia.uhrwerk;

import io.qimia.uhrwerk.backend.service.config.TableConfigService;
import io.qimia.uhrwerk.backend.service.dependency.TableDependencyService;

/**
 * Single interface unifying all the metastores interfaces
 */
public interface MetaStoreInt extends TableDependencyService, TableConfigService {}
// TODO: Rename
