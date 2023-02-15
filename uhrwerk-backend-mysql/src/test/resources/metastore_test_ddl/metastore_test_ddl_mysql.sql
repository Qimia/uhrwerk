USE UHRWERK_METASTORE_TEST2;
CREATE TABLE IF NOT EXISTS CONNECTION
(
    id                    BIGINT AUTO_INCREMENT PRIMARY KEY,
    name                  VARCHAR(256)                                       NOT NULL,
    type                  enum ('FS', 'JDBC', 'S3', 'GC', 'ABS', 'REDSHIFT') NOT NULL,
    path                  VARCHAR(512)                                       NULL,
    jdbc_url              VARCHAR(512)                                       NULL,
    jdbc_driver           VARCHAR(512)                                       NULL,
    jdbc_user             VARCHAR(512)                                       NULL,
    jdbc_pass             VARCHAR(512)                                       NULL,
    aws_access_key_id     VARCHAR(512)                                       NULL,
    aws_secret_access_key VARCHAR(512)                                       NULL,
    redshift_format       VARCHAR(512)                                       NULL,
    redshift_aws_iam_role VARCHAR(512)                                       NULL,
    redshift_temp_dir     VARCHAR(512)                                       NULL,
    deactivated_ts        TIMESTAMP                                          NULL,
    deactivated_epoch     BIGINT AS (IFNULL((TIMESTAMPDIFF(second, '1970-01-01', deactivated_ts)),
    0)),
    created_ts            TIMESTAMP DEFAULT CURRENT_TIMESTAMP                NULL,
    updated_ts            TIMESTAMP DEFAULT CURRENT_TIMESTAMP                NULL ON update CURRENT_TIMESTAMP,
    description           VARCHAR(512)                                       NULL,
    hash_key              BIGINT                                             NOT NULL,
    INDEX (hash_key),
    UNIQUE (name, deactivated_epoch)
    );

CREATE TABLE IF NOT EXISTS SECRET_
(
    id                BIGINT AUTO_INCREMENT PRIMARY KEY,
    name              VARCHAR(128)                        NOT NULL,
    type              enum ('AWS','AZURE','GCP')          NOT NULL,
    aws_secret_name   VARCHAR(512)                        NULL,
    aws_region        VARCHAR(32)                         NULL,
    deactivated_ts    TIMESTAMP                           NULL,
    deactivated_epoch BIGINT AS (IFNULL((TIMESTAMPDIFF(second, '1970-01-01', deactivated_ts)),
    0)),
    created_ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON update CURRENT_TIMESTAMP,
    description       VARCHAR(512)                        NULL,
    hash_key          BIGINT                              NOT NULL,
    INDEX (hash_key),
    UNIQUE (name, deactivated_epoch)
    );

CREATE TABLE IF NOT EXISTS TABLE_
(
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    area                VARCHAR(128)                               NOT NULL,
    vertical            VARCHAR(128)                               NOT NULL,
    name                VARCHAR(128)                               NOT NULL,
    version             VARCHAR(128)                               NOT NULL,
    partition_unit      enum ('WEEKS', 'DAYS', 'HOURS', 'MINUTES') NULL,
    partition_size      int                                        NOT NULL,
    parallelism         int                                        NOT NULL,
    max_bulk_size       int                                        NOT NULL,
    class_name          VARCHAR(128)                               NOT NULL,
    transform_sql_query TEXT      DEFAULT NULL,
    partitioned         BOOLEAN                                    NOT NULL DEFAULT FALSE,
    partition_columns   JSON                                       NULL,
    table_variables     JSON                                       NULL,
    deactivated_ts      TIMESTAMP                                  NULL,
    deactivated_epoch   BIGINT AS (IFNULL((TIMESTAMPDIFF(second, '1970-01-01', deactivated_ts)),
    0)),
    created_ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP        NULL,
    updated_ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP        NULL ON update CURRENT_TIMESTAMP,
    description         VARCHAR(512)                               NULL,
    hash_key            BIGINT                                     NOT NULL,
    INDEX (hash_key),
    UNIQUE (area, vertical, name, version, deactivated_epoch)
    );

CREATE TABLE IF NOT EXISTS SOURCE
(
    id                        BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id                  BIGINT                           NOT NULL,
    table_key                 BIGINT                           NOT NULL,
    connection_key            BIGINT                           NOT NULL,
    path                      VARCHAR(512)                     NOT NULL,
    format                    VARCHAR(64)                      NOT NULL,
    ingestion_mode            enum ('INTERVAL','DELTA', 'ALL') NOT NULL DEFAULT 'ALL',
    interval_temp_unit        enum ('DAYS','HOURS','MINUTES')  NULL,
    interval_temp_size        int                              NULL,
    interval_column           VARCHAR(256)                              DEFAULT NULL,
    delta_column              VARCHAR(256)                              DEFAULT NULL,
    select_query              TEXT                                      DEFAULT NULL,
    source_variables          JSON                             NULL,
    parallel_load             BOOLEAN                          NOT NULL DEFAULT FALSE,
    parallel_partition_query  TEXT                                      DEFAULT NULL,
    parallel_partition_column VARCHAR(256)                              DEFAULT NULL,
    parallel_partition_num    INT                              NOT NULL,
    auto_load                 BOOLEAN                          NOT NULL DEFAULT TRUE,
    deactivated_ts            TIMESTAMP                        NULL,
    deactivated_epoch         BIGINT AS (IFNULL((TIMESTAMPDIFF(second, '1970-01-01', deactivated_ts)),
    0)),
    created_ts                TIMESTAMP                                 DEFAULT CURRENT_TIMESTAMP,
    updated_ts                TIMESTAMP                                 DEFAULT CURRENT_TIMESTAMP ON update CURRENT_TIMESTAMP,
    description               VARCHAR(512),
    hash_key                  BIGINT                           NOT NULL,
    INDEX (hash_key),
    INDEX (table_key),
    UNIQUE (table_key, connection_key, path, format, deactivated_epoch),
    FOREIGN KEY (table_id) REFERENCES TABLE_ (id) ON DELETE CASCADE
    );


CREATE TABLE IF NOT EXISTS TARGET
(
    id                BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id          BIGINT                              NOT NULL,
    table_key         BIGINT                              NOT NULL,
    connection_key    BIGINT                              NOT NULL,
    format            VARCHAR(64)                         NOT NULL,
    table_name        VARCHAR(512)                        NULL,
    deactivated_ts    TIMESTAMP                           NULL,
    deactivated_epoch BIGINT AS (IFNULL((TIMESTAMPDIFF(second, '1970-01-01', deactivated_ts)),
    0)),
    created_ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON update CURRENT_TIMESTAMP,
    hash_key          BIGINT                              NOT NULL,
    INDEX (hash_key),
    INDEX (table_key),
    INDEX (table_key, format),
    UNIQUE (table_key, connection_key, format, deactivated_epoch),
    FOREIGN KEY (table_id) REFERENCES TABLE_ (id) ON DELETE CASCADE
    );

CREATE TABLE IF NOT EXISTS DEPENDENCY
(
    id                    BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id              BIGINT                              NOT NULL,
    table_key             BIGINT                              NOT NULL,
    dependency_target_key BIGINT                              NOT NULL,
    dependency_table_key  BIGINT                              NOT NULL,
    view_name             VARCHAR(128)                        NULL,
    partition_mappings    JSON                                NULL,
    dependency_variables  JSON                                NULL,
    deactivated_ts        TIMESTAMP                           NULL,
    deactivated_epoch     BIGINT AS (IFNULL((TIMESTAMPDIFF(second, '1970-01-01', deactivated_ts)),
    0)),
    created_ts            TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts            TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON update CURRENT_TIMESTAMP,
    description           VARCHAR(512)                        NULL,
    hash_key              BIGINT                              NOT NULL,
    INDEX (hash_key),
    INDEX (table_key),
    INDEX (dependency_table_key),
    UNIQUE (table_key, dependency_table_key, dependency_target_key, deactivated_epoch),
    FOREIGN KEY (table_id) REFERENCES TABLE_ (id) ON DELETE CASCADE
    );

CREATE TABLE IF NOT EXISTS PARTITION_
(
    id                BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_key         BIGINT        NOT NULL,
    target_key        BIGINT        NOT NULL,
    partition_ts      TIMESTAMP     NOT NULL,
    partitioned       BOOLEAN       NOT NULL DEFAULT FALSE,
    bookmarked        BOOLEAN       NOT NULL DEFAULT FALSE,
    max_bookmark      VARCHAR(128)  NULL,
    partition_values  JSON          NULL,
    partition_path    VARCHAR(1024) NULL,
    deactivated_ts    TIMESTAMP     NULL,
    deactivated_epoch BIGINT AS (IFNULL((TIMESTAMPDIFF(second, '1970-01-01', deactivated_ts)),
    0)),
    created_ts        TIMESTAMP              DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts        TIMESTAMP              DEFAULT CURRENT_TIMESTAMP NULL ON update CURRENT_TIMESTAMP,
    INDEX (partition_ts),
    INDEX (max_bookmark),
    index (table_key),
    index (target_key)
    );

CREATE TABLE IF NOT EXISTS PARTITION_DEPENDENCY
(
    id                      BIGINT AUTO_INCREMENT PRIMARY KEY,
    partition_id            BIGINT                              NOT NULL,
    dependency_partition_id BIGINT                              NOT NULL,
    created_ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON update CURRENT_TIMESTAMP,
    UNIQUE (partition_id, dependency_partition_id),
    FOREIGN KEY (partition_id) REFERENCES PARTITION_ (id) ON DELETE CASCADE,
    foreign key (dependency_partition_id) REFERENCES PARTITION_ (id) ON DELETE CASCADE
    );