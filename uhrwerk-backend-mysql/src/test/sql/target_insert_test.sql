INSERT INTO CONNECTION(name, type, path)
VALUES ('fs_connection_1', 'JDBC', '/some/test/path');

INSERT INTO TABLE_(area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)
VALUES ('area1', 'vertical1', 'name1', '1.0', 'MINUTES', 15, 4, 32)
ON DUPLICATE KEY UPDATE parallelism= 4,
                        max_bulk_size=32;

SELECT id
FROM TABLE_
WHERE area = 'area1'
  AND vertical = 'vertical1'
  AND name = 'name1'
  AND version = '1.0';

INSERT INTO TARGET (table_id, connection_id, format)
SELECT 1, cn.id, 'json'
FROM CONNECTION cn
WHERE cn.name = 'fs_connection_1';

SELECT tr.* from TARGET tr
JOIN CONNECTION cn on cn.id = tr.connection_id
WHERE cn.name = 'fs_connection_1';


