USE
UHRWERK_METASTORE;
-- TABLE_ --
ALTER TABLE TABLE_
    ADD COLUMN partition_columns JSON NULL AFTER partitioned;

ALTER TABLE TABLE_
    ADD COLUMN table_variables JSON NULL AFTER partition_columns;

-- SOURCE --
ALTER TABLE SOURCE
    ADD COLUMN source_variables JSON NULL AFTER select_query;

-- DEPENDENCY --
ALTER TABLE DEPENDENCY
    ADD COLUMN partition_mappings JSON NULL AFTER view_name;

ALTER TABLE DEPENDENCY
    ADD COLUMN dependency_variables JSON NULL AFTER partition_mappings;

-- PARTITION_ --
ALTER TABLE PARTITION_
    ADD COLUMN partition_values JSON NULL AFTER max_bookmark;

ALTER TABLE PARTITION_
    ADD COLUMN partition_path VARCHAR(1024) NULL AFTER partition_values;
