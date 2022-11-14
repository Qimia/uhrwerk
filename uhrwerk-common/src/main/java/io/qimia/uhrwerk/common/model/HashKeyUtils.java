package io.qimia.uhrwerk.common.model;

import net.openhft.hashing.LongHashFunction;

public class HashKeyUtils {

  public static Long targetKey(TargetModel target) {
    assert target.tableId != null : "Target.tableId can't be null";
    assert target.connectionId != null : "Target.connectionId can't be null";
    assert target.format != null && !target.format.isEmpty()
        : "Target.format can't be null or empty";

    return hashKey(
        new StringBuilder()
            .append(target.tableId)
            .append(target.connectionId)
            .append(target.format));
  }

  public static Long tableKey(TableModel table) {
    assert table.area != null && !table.area.isEmpty() : "Table.area can't be null or empty";
    assert table.vertical != null && !table.vertical.isEmpty()
        : "Table.vertical can't be null or empty";
    assert table.name != null && !table.name.isEmpty() : "Table.name can't be null or empty";
    assert table.version != null && !table.version.isEmpty()
        : "Table.version can't be null or empty";

    return hashKey(
        new StringBuilder()
            .append(table.area)
            .append(table.vertical)
            .append(table.name)
            .append(table.version));
  }

  public static Long tableKey(DependencyModel dependency) {
    assert dependency.area != null && !dependency.area.isEmpty()
        : "Dependency.area can't be null or empty";
    assert dependency.vertical != null && !dependency.vertical.isEmpty()
        : "Dependency.vertical can't be null or empty";
    assert dependency.tableName != null && !dependency.tableName.isEmpty()
        : "Dependency.tableName can't be null or empty";
    assert dependency.version != null && !dependency.version.isEmpty()
        : "Dependency.version can't be null or empty";

    return hashKey(
        new StringBuilder()
            .append(dependency.area)
            .append(dependency.vertical)
            .append(dependency.tableName)
            .append(dependency.version));
  }

  public static Long sourceKey(SourceModel source) {
    assert source.tableId != null : "Source.tableId can't be null";
    assert source.connectionId != null : "Source.connectionId can't be null";
    assert source.path != null && !source.path.isEmpty() : "Source.path can't be null or empty";
    assert source.format != null && !source.format.isEmpty()
        : "Source.format can't be null or empty";
    return hashKey(
        new StringBuilder()
            .append(source.tableId)
            .append(source.connectionId)
            .append(source.path)
            .append(source.format));
  }

  public static Long dependencyKey(DependencyModel dependency) {
    assert dependency.tableId != null : "Dependency.tableId can't be null";
    assert dependency.dependencyTargetId != null : "Dependency.dependencyTargetId can't be null";
    assert dependency.dependencyTableId != null : "Dependency.dependencyTableId can't be null";
    return hashKey(
        new StringBuilder()
            .append(dependency.tableId)
            .append(dependency.dependencyTargetId)
            .append(dependency.dependencyTableId));
  }

  public static Long connectionKey(ConnectionModel connection) {
    assert connection.name != null && !connection.name.isEmpty()
        : "Connection.name can't be null or empty";
    return hashKey(new StringBuilder().append(connection.name));
  }

  private static Long hashKey(StringBuilder strBuilder) {
    return LongHashFunction.xx().hashChars(strBuilder);
  }
}
