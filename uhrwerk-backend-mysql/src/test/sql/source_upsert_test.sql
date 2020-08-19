INSERT INTO SOURCE(id, table_id, connection_id, path, format, partition_unit, partition_size, sql_select_query, sql_partition_query,
                   partition_column, partition_num, query_column)
VALUES(42,
       1,
       1,
       'path',
       'format',
       'MINUTES',
       15,
       'query',
       'query2',
       'column',
       40,
       'column2');

SELECT * FROM SOURCE;
select * from TABLE_;
select * from CONNECTION;

# INSERT INTO CONNECTION(id, name, type, path)
# VALUES(-172160285628269299, 'Test-Conn-Source', 'FS', '/some/path/test1');

