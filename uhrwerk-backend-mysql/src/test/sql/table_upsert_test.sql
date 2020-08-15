INSERT INTO TABLE_(area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)
VALUES ('area1', 'vertical1', 'name1', '1.0', 'MINUTES', 15, 4, 32)
ON DUPLICATE KEY UPDATE parallelism= 4,
                        max_bulk_size=32;

SELECT area,
       vertical,
       name,
       version,
       partition_unit,
       partition_size,
       parallelism,
       max_bulk_size,
       created_ts,
       updated_ts
FROM TABLE_
WHERE area = 'area1'
  AND vertical = 'vertical1'
  AND name = 'name1';

INSERT INTO TABLE_(area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)
VALUES ('area1', 'vertical1', 'name1', '1.0', 'HOURS', 2, 8, 64)
ON DUPLICATE KEY UPDATE parallelism= 8,
                        max_bulk_size=64;

INSERT INTO TABLE_(area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)
VALUES ('area1', 'vertical1', 'name1', '1.1', 'HOURS', 2, 8, 64)
ON DUPLICATE KEY UPDATE parallelism= 8,
                        max_bulk_size=64;