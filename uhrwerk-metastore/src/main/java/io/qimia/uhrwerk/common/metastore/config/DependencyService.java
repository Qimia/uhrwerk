package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.metastore.model.DependencyModel;
import io.qimia.uhrwerk.common.metastore.model.TableModel;
import java.util.List;

public interface DependencyService {

  /**
   * Save all dependencies for a given table
   *
   * @param tableId    the table ID
   * @param overwrite overwrite the previously stored dependencies or not
   * @return DependencyStoreResult object with stored objects, info about success, exceptions and
   * other results
   */
  DependencyStoreResult save(Long tableId,
      DependencyModel[] dependencies,
      boolean overwrite);

  /**
   * Retrieve all stored dependencies for a given table
   *
   * @param tableId tableId of the table for which the dependencies are returned
   * @return model Dependency objects
   */
  List<DependencyModel> getByTableId(Long tableId);

  Integer deactivateByTableId(long tableId);
}
