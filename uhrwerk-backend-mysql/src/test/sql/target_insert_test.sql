INSERT INTO CONNECTION(id, name, type, path)
VALUES (123, 'fs_connection_1', 'JDBC', '/some/test/path');

INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)
VALUES (234, 'area1', 'vertical1', 'name1', '1.0', 'MINUTES', 15, 4, 32);

INSERT INTO TARGET (id, table_id, connection_id, format)
(SELECT 345, 234, cn.id, 'jdbc'
FROM CONNECTION cn
WHERE cn.name = 'fs_connection_1');

SELECT tr.id, tr.table_id, tr.connection_id, tr.format
FROM TARGET tr
JOIN CONNECTION cn on cn.id = tr.connection_id
WHERE cn.name = 'fs_connection_1';      -- Assumes Connection names are unique (!!)

-- Target's id is set based on the tableId and format
SELECT *
FROM TARGET
WHERE id = 345;




