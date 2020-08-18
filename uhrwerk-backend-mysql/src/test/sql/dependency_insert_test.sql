-- Setup required objects in store
-- (we need a target to depend on)
INSERT INTO CONNECTION(name, type, path)
VALUES ('a_connection', 'fs', '/some/test/path');  -- Id 1

INSERT INTO TABLE_(area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)
VALUES ('area1', 'vertical1', 'name1', '1.0', 'HOURS', 1, 1, 1); -- Id 1

INSERT INTO TARGET (table_id, connection_id, format)
VALUES (1, 1, "parquet");  -- Check ids

-- New table is inserted before the dependency
INSERT INTO TABLE_(area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)
VALUES ('area1', 'vertical1', 'name2', '1.0', 'HOURS', 1, 1, 1); -- Id 2

-- Insert a correct new Dependency (we also check the partition size here to see if it is correct or not!!)
INSERT INTO DEPENDENCY(table_id, target_id, transform_type)
(SELECT
    2, tar.id, "identity"
FROM TABLE_ tab
JOIN TARGET tar
ON tar.table_id = tab.id
WHERE tab.area = "area1"
AND tab.vertical = "vertical1"
AND tab.name = "name1"
AND tab.version = "1.0"
AND tar.format = "parquet"
AND tab.partition_size = 1
AND tab.partition_unit = "HOURS");

TRUNCATE DEPENDENCY;

-- Insert a bad dependency (because of a wrong partition size for the table)
INSERT INTO DEPENDENCY(table_id, target_id, transform_type)
(SELECT
    2, tar.id, "identity"
FROM TABLE_ tab
JOIN TARGET tar
ON tar.table_id = tab.id
WHERE tab.area = "area1"
AND tab.vertical = "vertical1"
AND tab.name = "name1"
AND tab.version = "1.0"
AND tar.format = "parquet"
AND tab.partition_size = 1
AND tab.partition_unit = "MINUTES");

-- Replacing Dependencies means removing old and inserting new
DELETE FROM DEPENDENCY
WHERE table_id = 2;

-- (Redo creating the dependency)
INSERT INTO DEPENDENCY(table_id, target_id, transform_type, transform_partition_size)
(SELECT
    2, tar.id, "identity", "hours", 1
FROM TABLE_ tab
JOIN TARGET tar
ON tar.table_id = tab.id
WHERE tab.area = "area1"
AND tab.vertical = "vertical1"
AND tab.name = "name1"
AND tab.version = "1.0"
AND tar.format = "parquet");

-- Checking if a dependency is there
-- (When we don't want to overwrite we don't allow any changes, if the table is otherwise the same)
-- (Later 2 versions, one with known target_id and one without)
-- Assumes the partition-size of the dependency's table is already checked on insertion
SELECT id, target_id
FROM DEPENDENCY
WHERE table_id = 2
AND target_id = (
    SELECT tar.id
    FROM TABLE_ tab
    JOIN TARGET tar
    ON tar.table_id = tab.id
    WHERE tab.area = "area1"
    AND tab.vertical = "vertical1"
    AND tab.name = "name1"
    AND tab.version = "1.0"
    AND tar.format = "parquet"
)
AND transform_type = "identity"
AND transform_partition_unit = "hours"
AND transform_partition_size = 1;


