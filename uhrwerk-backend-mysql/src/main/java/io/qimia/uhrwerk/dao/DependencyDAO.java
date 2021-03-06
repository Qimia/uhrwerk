package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.PartitionDurationTester;
import io.qimia.uhrwerk.common.metastore.config.DependencyStoreResult;
import io.qimia.uhrwerk.common.metastore.config.DependencyStoreService;
import io.qimia.uhrwerk.common.model.Dependency;
import io.qimia.uhrwerk.common.model.PartitionTransformType;
import io.qimia.uhrwerk.common.model.PartitionUnit;
import io.qimia.uhrwerk.common.model.Table;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class DependencyDAO implements DependencyStoreService {

  private static final String GET_TABLE_BY_TARGET =
      "SELECT tab.id, tab.name, tab.partitioned, tab.partition_unit, tab.partition_size"
          + " FROM TARGET tar JOIN TABLE_ tab ON tar.table_id = tab.id WHERE tar.id IN (%s)";

  private static String checkTableByTargetQuery(Long[] targetIds) {
    var concatIds = Arrays.toString(targetIds);
    return String.format(GET_TABLE_BY_TARGET, concatIds.substring(1, concatIds.length() - 1));
  }

  private static final String DELETE_DEPENDENCIES = "DELETE FROM DEPENDENCY WHERE table_id = ?";

  private static final String INSERT_DEPENDENCY =
          "INSERT INTO DEPENDENCY "
                  + "(id, table_id, dependency_target_id, dependency_table_id, transform_type, transform_partition_unit, "
                  + "transform_partition_size) VALUES (?, ?, ?, ?, ?, ?, ?)";

  private static final String GET_DEPENDENCY_BY_TABLE =
          "SELECT dep.id, dep.table_id, dep.dependency_target_id, "
                  + "dep.dependency_table_id, tab.area, tab.vertical, tab.name, tar.format, tab.version, dep.transform_type, "
                  + "dep.transform_partition_unit, dep.transform_partition_size FROM DEPENDENCY dep "
                  + "JOIN TABLE_ tab ON dep.dependency_table_id = tab.id "
                  + "JOIN TARGET tar ON dep.dependency_target_id = tar.id "
                  + "WHERE dep.table_id = ?";

  static class DepCompareRes {
    boolean success;
    String problem;
  }

  /**
   * Compare two dependencies and report any discrepancies
   *
   * @param trueDep the gold standard dependency
   * @param newDep the other dependency which is compared
   * @return Result denoting success or not, and if not what is exactly different
   */
  public static DepCompareRes compareDependency(Dependency trueDep, Dependency newDep) {
    var result = new DepCompareRes();
    result.success = true;

    StringBuilder problemString = new StringBuilder();
    String newDepName = newDep.getTableName();
    if (!trueDep.getTableId().equals(newDep.getTableId())) {
      result.success = false;
      problemString
          .append("dependency: ")
          .append(newDepName)
          .append("\thas a different table-id\n");
    }
    if (!trueDep.getArea().equals(newDep.getArea())) {
      result.success = false;
      problemString.append("dependency: ").append(newDepName).append("\thas a different area\n");
    }
    if (!trueDep.getVertical().equals(newDep.getVertical())) {
      result.success = false;
      problemString
          .append("dependency: ")
          .append(newDepName)
          .append("\thas a different vertical\n");
    }
    if (!trueDep.getTableName().equals(newDep.getTableName())) {
      result.success = false;
      problemString
          .append("dependency: ")
          .append(newDepName)
          .append("\thas a different table name\n");
    }
    if (!trueDep.getFormat().equals(newDep.getFormat())) {
      result.success = false;
      problemString.append("dependency: ").append(newDepName).append("\thas a different format\n");
    }
    if (!trueDep.getVersion().equals(newDep.getVersion())) {
      result.success = false;
      problemString.append("dependency: ").append(newDepName).append("\thas a different version\n");
    }
    if (trueDep.getTransformType() != newDep.getTransformType()) {
      result.success = false;
      problemString.append("dependency: ").append(newDepName).append("\thas a different type\n");
    }

    if (trueDep.getTransformPartitionSize() != newDep.getTransformPartitionSize()) {
      result.success = false;
      problemString.append("dependency: ").append(newDepName).append("\thas a different size\n");
    }
    // All other ID's in Dependency are set based on these parameters (and should be the same if
    // these are the same)

    if (!result.success) {
      result.problem = problemString.toString();
    }
    return result;
  }

  /**
   * Test the dependencies partition sizes by giving the partition size of the target table and the
   * information of the dependencies' tables and the dependencies(' transformations)
   *
   * @param dependencies dependencies that need to be tested
   * @param partitionUnit the partition unit of the output table
   * @param partitionSize the partition size (how many times unit) of the output table
   * @param dependencyTables info about the partition size of each of the dependencies' tables
   * @return PartitionTestResult showing if they were all good or which tables (-names) did not
   *     match up
   */
  public static PartitionDurationTester.PartitionTestResult checkPartitionSizes(
      Dependency[] dependencies,
      PartitionUnit partitionUnit,
      int partitionSize,
      List<TablePartRes> dependencyTables) {
    HashMap<Long, Dependency> depTableIdLookup = new HashMap<>();
    for (Dependency d : dependencies) {
      depTableIdLookup.put(d.getDependencyTableId(), d);
    }
    ArrayList<PartitionDurationTester.PartitionTestDependencyInput> partitionTestInputs =
        new ArrayList<>();
    for (TablePartRes tableRes : dependencyTables) {
      var newTest = new PartitionDurationTester.PartitionTestDependencyInput();
      var dependency = depTableIdLookup.get(tableRes.tableId);
      newTest.dependencyTableName = dependency.getTableName();
      newTest.transformType = dependency.getTransformType();
      newTest.transformSize = dependency.getTransformPartitionSize();
      // TODO: Doesn't support transform-partition-unit (and transforms using that) for now
      newTest.dependencyTablePartitionSize = tableRes.partitionSize;
      newTest.dependencyTablePartitionUnit = tableRes.partitionUnit;
      newTest.dependencyTablePartitioned = tableRes.partitioned;
      partitionTestInputs.add(newTest);
    }
    return PartitionDurationTester.checkDependencies(
        partitionUnit,
        partitionSize,
        partitionTestInputs.toArray(new PartitionDurationTester.PartitionTestDependencyInput[0]));
  }

  private final java.sql.Connection db;

  public DependencyDAO(java.sql.Connection db) {
    this.db = db;
  }

  static class ExistingDepRes {
    boolean found;
    boolean correct;
    String problems;
  }

  /**
   * Check if there are already dependencies stored for a table and see if they match the
   * dependencies given for storage to DependencyDAO
   *
   * @param tableId id of table for which the dependencies are defined
   * @param dependencies dependencies given that need to be checked
   * @return The result of this check showing if the dependencies were found, if the found ones are
   *     correct. If they are not correct, shows what exactly is wrong (can be multi line String!)
   */
  public ExistingDepRes checkExistingDependencies(Long tableId, Dependency[] dependencies) {
    var existingRes = new ExistingDepRes();
    var storedDeps = get(tableId);

    if (storedDeps.length == 0) {
      existingRes.found = false;
      existingRes.correct = true;
      return existingRes;
    }

    // If found, match with given dependencies and check if they are the same
    existingRes.found = true;
    existingRes.correct = true;
    existingRes.problems = "";

    // TODO: Assumes tablenames unique in the context of a single table's dependencies
    // see findTables comment
    HashMap<String, Dependency> storedDepLookup = new HashMap<>();
    for (Dependency storedDep : storedDeps) {
      storedDepLookup.put(storedDep.getTableName(), storedDep);
    }
    StringBuilder problemString = new StringBuilder();
    for (Dependency inDep : dependencies) {
      var depTableName = inDep.getTableName();
      if (!storedDepLookup.containsKey(depTableName)) {
        existingRes.correct = false;
        problemString.append("dependency:").append(depTableName).append("\twas later added\n");
        continue;
      }
      var foundDep = storedDepLookup.get(depTableName);
      var compareRes = compareDependency(foundDep, inDep);
      if (!compareRes.success) {
        existingRes.correct = false;
        problemString.append(compareRes.problem);
      }
    }
    if (!existingRes.correct) {
      existingRes.problems = problemString.toString();
    }
    return existingRes;
  }

  static class TablePartRes {
    public Long tableId;
    public boolean partitioned;
    public PartitionUnit partitionUnit;
    public int partitionSize;
  }

  static class FindTableRes {
    List<TablePartRes> foundTables;
    Set<String> missingNames;
  }

  /**
   * Find the tables for all the dependencies and check if the targets exists by querying on
   * target-id.
   *
   * @param dependencies dependencies that have to be found
   * @return FindQueryResult object with partition info about found tables and the names of the
   *     missing tables
   * @throws SQLException can throw database query errors
   * @throws IllegalArgumentException
   */
  public FindTableRes findTables(Dependency[] dependencies)
      throws SQLException, IllegalArgumentException {
    Long[] targetIds =
        Arrays.stream(dependencies).map(Dependency::getDependencyTargetId).toArray(Long[]::new);
    Set<String> namesToFind =
        Arrays.stream(dependencies).map(Dependency::getTableName).collect(Collectors.toSet());

    // TODO: Assumes tablenames unique in the context of a single table's dependencies
    // now assumes yes but in theory could be no -> then we need to map the missing target-ids back
    // to full table info

    Statement statement = db.createStatement();
    String query = DependencyDAO.checkTableByTargetQuery(targetIds);
    ResultSet rs = statement.executeQuery(query);

    ArrayList<TablePartRes> foundTables = new ArrayList<>(dependencies.length);
    while (rs.next()) {
      var singleRes = new TablePartRes();
      singleRes.tableId = rs.getLong("tab.id");
      var partitionUnit = rs.getString("tab.partition_unit");
      if ((partitionUnit != null) && (!partitionUnit.equals(""))) {
        singleRes.partitionUnit = PartitionUnit.valueOf(partitionUnit.toUpperCase());
      }
      singleRes.partitionSize = rs.getInt("tab.partition_size");
      singleRes.partitioned = rs.getBoolean("tab.partitioned");
      namesToFind.remove(rs.getString("tab.name"));
      foundTables.add(singleRes);
    }

    FindTableRes results = new FindTableRes();
    results.foundTables = foundTables;
    results.missingNames = namesToFind;
    return results;
  }

  /**
   * Delete all dependencies for a given tableId
   *
   * @param tableId id of the table for which to delete the dependencies
   * @throws SQLException can throw database query errors
   */
  public void deleteAllDependencies(Long tableId) throws SQLException {
    PreparedStatement statement = db.prepareStatement(DELETE_DEPENDENCIES);
    statement.setLong(1, tableId);
    statement.executeUpdate();
  }

  /**
   * Insert all dependencies (assumes no key collisions!)
   *
   * @param dependencies array of dependencies
   * @throws SQLException can throw database query errors
   */
  public void insertDependencies(Dependency[] dependencies) throws SQLException {
    PreparedStatement statement = db.prepareStatement(INSERT_DEPENDENCY);
    for (Dependency dependency : dependencies) {
      statement.setLong(1, dependency.getId());
      statement.setLong(2, dependency.getTableId());
      statement.setLong(3, dependency.getDependencyTargetId());
      statement.setLong(4, dependency.getDependencyTableId());
      statement.setString(5, dependency.getTransformType().toString());
      var tranformPartitionUnit = dependency.getTransformPartitionUnit();
      if (tranformPartitionUnit != null) {
        statement.setString(6, tranformPartitionUnit.toString());
      } else {
        statement.setNull(6, Types.VARCHAR);
      }
      statement.setInt(7, dependency.getTransformPartitionSize());
      statement.addBatch();
    }
    statement.executeBatch();
  }

  /**
   * Save all dependencies for a given table
   *
   * @param table the table
   * @param overwrite overwrite the previously stored dependencies or not
   * @return DependencyStoreResult object with stored objects, info about success, exceptions and
   *     other results
   */
  @Override
  public DependencyStoreResult save(Table table, boolean overwrite) {
    // Assumes that new version means new tableId and thus any dependencies for the old version
    // won't be found
    DependencyStoreResult result = new DependencyStoreResult();
    var dependencies = table.getDependencies();

    if (!overwrite) {
      // check if there are stored dependencies and if they are correct
      ExistingDepRes checkDeps = checkExistingDependencies(table.getId(), dependencies);
      if (checkDeps.found && !checkDeps.correct) {
        result.setSuccess(false);
        result.setError(false);
        result.setMessage(checkDeps.problems);
        return result;
      } else if (checkDeps.found && checkDeps.correct) {
        result.setSuccess(true);
        result.setError(false);
        result.setDependenciesSaved(dependencies);
        return result;
      }
      // if none are found continue adding the dependencies (and skip the remove-old-dependencies
      // step)
    }

    // overwrite or write needed -> first get all tables (abort if table not found)
    FindTableRes tableSearchRes;
    try {
      tableSearchRes = findTables(dependencies);
    } catch (Exception e) {
      result.setError(true);
      result.setException(e);
      result.setMessage(e.getMessage());
      return result;
    }
    if (!tableSearchRes.missingNames.isEmpty()) {
      result.setSuccess(false);
      result.setMessage(
          "Missing tables: " + tableSearchRes.missingNames.toString() + " (based on id)");
      return result;
    }

    if (table.isPartitioned()) {
      // Check dependency sizes (abort if size does not match)
      var sizeTestResult =
          checkPartitionSizes(
              dependencies,
              table.getPartitionUnit(),
              table.getPartitionSize(),
              tableSearchRes.foundTables);
      if (!sizeTestResult.success) {
        result.setSuccess(false);
        result.setError(false);
        result.setMessage(
            String.format(
                "Tables %s have the wrong partition duration",
                Arrays.toString(sizeTestResult.badTableNames)));
        return result;
      }
    } else {
      // Check if there are any dependencies which have a transformation (and/or are themselves
      // partitioned)
      boolean problem = false;
      ArrayList<String> problemDependencies = new ArrayList<>();
      var tablePartitionLookup = new HashMap<Long, TablePartRes>();
      for (TablePartRes foundTable : tableSearchRes.foundTables) {
        tablePartitionLookup.put(foundTable.tableId, foundTable);
      }
      for (Dependency checkDep : dependencies) {
        TablePartRes connectedTable = tablePartitionLookup.get(checkDep.getDependencyTableId());
        if (!checkDep.getTransformType().equals(PartitionTransformType.NONE)
            || (connectedTable.partitioned)) {
          problem = true;
          problemDependencies.add(checkDep.getTableName());
        }
      }
      if (problem) {
        result.setSuccess(false);
        result.setError(false);
        result.setMessage(
            "Tables " + String.join(", ", problemDependencies) + " have a partitioning problem");
        return result;
      }
    }
    // If it is a table without partitioning, the fact that they exist should be enough

    // Delete all old dependencies (in case of overwrite) and insert new list
    try {
      if (overwrite) {
        deleteAllDependencies(table.getId());
      }
      insertDependencies(dependencies);
      result.setSuccess(true);
      result.setError(false);
      result.setDependenciesSaved(dependencies);
    } catch (Exception e) {
      result.setSuccess(false);
      result.setError(true);
      result.setException(e);
      result.setMessage(e.getMessage());
    }
    return result;
  }

  /**
   * Retrieve all stored dependencies for a given table
   *
   * @param tableId tableId of the table for which the dependencies are returned
   * @return model Dependency objects
   */
  @Override
  public Dependency[] get(Long tableId) {
    ArrayList<Dependency> dependencies = new ArrayList<>();
    try {
      PreparedStatement statement = db.prepareStatement(GET_DEPENDENCY_BY_TABLE);
      statement.setLong(1, tableId);
      ResultSet rs = statement.executeQuery();
      while (rs.next()) {
        var singleDep = new Dependency();
        singleDep.setId(rs.getLong("dep.id"));
        singleDep.setTableId(rs.getLong("dep.table_id"));
        singleDep.setDependencyTargetId(rs.getLong("dep.dependency_target_id"));
        singleDep.setDependencyTableId(rs.getLong("dep.dependency_table_id"));
        singleDep.setArea(rs.getString("tab.area"));
        singleDep.setVertical(rs.getString("tab.vertical"));
        singleDep.setTableName(rs.getString("tab.name"));
        singleDep.setFormat(rs.getString("tar.format"));
        singleDep.setVersion(rs.getString("tab.version"));
        var transformType = rs.getString("dep.transform_type");
        if ((transformType == null) || transformType.equals("")) {
          singleDep.setTransformType(null);
        } else {
          singleDep.setTransformType(PartitionTransformType.valueOf(transformType.toUpperCase()));
        }
        singleDep.setTransformPartitionSize(rs.getInt("dep.transform_partition_size"));
        var transformPartitionUnit = rs.getString("dep.transform_partition_unit");
        if ((transformPartitionUnit == null) || transformPartitionUnit.equals("")) {
          singleDep.setTransformPartitionUnit(null);
        } else {
          singleDep.setTransformPartitionUnit(PartitionUnit.valueOf(transformPartitionUnit));
        }
        dependencies.add(singleDep);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return dependencies.toArray(new Dependency[0]);
  }
}
