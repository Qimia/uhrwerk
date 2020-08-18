INSERT INTO SOURCE(table_id, connection_id, path, partition_unit, partition_size, sql_select_query, sql_partition_query,
                   partition_column, partition_num, query_column)
SELECT t.id,
       connection.id,
       'path',
       'MINUTES',
       15,
       'query',
       'query2',
       'column',
       40,
       'column2'
FROM CONNECTION connection
CROSS JOIN TABLE_ t
WHERE connection.name = 'fs_connection_1' and
t.name = 'name1';

SELECT * FROM SOURCE;