-- Setup required objects in store
-- (we need a target to depend on)
INSERT INTO CONNECTION(id, name, type, path)
VALUES (123, 'a_connection', 'fs', '/some/test/path');  -- Id 1

INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)
VALUES (234, 'area1', 'vertical1', 'name1', '1.0', 'HOURS', 1, 1, 1); -- Id 1

INSERT INTO TARGET (id, table_id, connection_id, format)
VALUES (345, 234, 123, "parquet");  -- Check ids

-- New table is inserted before the dependency
INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)
VALUES (456, 'area1', 'vertical1', 'name2', '1.0', 'HOURS', 1, 1, 1); -- Id 2

-- Insert a correct new Dependency (we also check the partition size here to see if it is correct or not!!)
INSERT INTO DEPENDENCY(id, table_id, dependency_target_id, dependency_table_id, transform_type)
VALUES (567, 456, 345, 234, "identity");

TRUNCATE DEPENDENCY;

-- Checking if target exists and if table has right partition size
SELECT tar.id, tab.partition_unit, tab.partition_size
FROM TARGET tar
JOIN TABLE_ tab
ON tar.table_id = tab.id
WHERE tar.id = 345;

-- Replacing Dependencies means removing old and inserting new
DELETE FROM DEPENDENCY
WHERE table_id = 456;

-- (Redo creating the dependency)
INSERT INTO DEPENDENCY(table_id, target_id, transform_type, )
VALUES (456, 345, "window");

-- Checking if a dependency is there
-- (When we don't want to overwrite we don't allow any changes, if the table is otherwise the same)
-- (Later 2 versions, one with known target_id and one without)
-- Assumes the partition-size of the dependency's table is already checked on insertion
SELECT EXISTS (SELECT 1
FROM DEPENDENCY
WHERE table_id = 456
AND target_id = 345
AND transform_type = "identity");

-- Loading all dependencies from the storage
SELECT 
	dep.id,
	dep.table_id,
	dep.dependency_target_id,
	dep.dependency_table_id,
	tab.area,
	tab.vertical,
	tab.name,
	tar.format,
	tab.version,
	dep.transform_type,  -- TODO: Skipping transform unit
	dep.transform_partition_size 
FROM DEPENDENCY dep
JOIN TABLE_ tab ON dep.dependency_table_id = tab.id
JOIN TARGET tar ON dep.dependency_target_id = tar.id
WHERE dep.table_id = 456;

-- Cleaning up
DELETE FROM DEPENDENCY;
DELETE FROM TARGET;
DELETE FROM TABLE_;
DELETE FROM `CONNECTION`;

DELETE FROM SOURCE ;


