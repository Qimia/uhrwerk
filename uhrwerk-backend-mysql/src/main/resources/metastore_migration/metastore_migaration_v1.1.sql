USE UHRWERK_METASTORE;
-- -- CONNECTION ----
ALTER TABLE CONNECTION
    ADD COLUMN redshift_format VARCHAR(512) NULL AFTER aws_secret_access_key;
ALTER TABLE CONNECTION
    ADD COLUMN redshift_aws_iam_role VARCHAR(512) NULL AFTER redshift_format;
ALTER TABLE CONNECTION
    ADD COLUMN redshift_temp_dir VARCHAR(512) NULL AFTER redshift_aws_iam_role;
ALTER TABLE CONNECTION
    MODIFY COLUMN type enum ('FS', 'JDBC', 'S3', 'GC', 'ABS', 'REDSHIFT');

-- -- TARGET -- --
ALTER TABLE TARGET
    ADD COLUMN table_name VARCHAR(512) NULL AFTER format;