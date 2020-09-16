USE UHRWERK_METASTORE;
CREATE TABLE IF NOT EXISTS CONNECTION
(
    id                    BIGINT PRIMARY KEY,
    name                  VARCHAR(256) UNIQUE                    NOT NULL,
    type                  enum ('FS', 'JDBC', 'S3', 'GC', 'ABS') NOT NULL,
    path                  VARCHAR(512),
    jdbc_url              VARCHAR(512),
    jdbc_driver           VARCHAR(512),
    jdbc_user             VARCHAR(512),
    jdbc_pass             VARCHAR(512),
    aws_access_key_id     VARCHAR(512),
    aws_secret_access_key VARCHAR(512),
    created_ts            TIMESTAMP DEFAULT CURRENT_TIMESTAMP    NULL,
    updated_ts            TIMESTAMP DEFAULT CURRENT_TIMESTAMP    NULL ON UPDATE CURRENT_TIMESTAMP,
    description           VARCHAR(512)                           NULL
);

create table if not exists TABLE_
(
    id             BIGINT PRIMARY KEY,
    area           VARCHAR(128)                               NOT NULL,
    vertical       VARCHAR(128)                               NOT NULL,
    name           VARCHAR(128)                               NOT NULL,
    partition_unit enum ('WEEKS', 'DAYS', 'HOURS', 'MINUTES') NULL,
    partition_size int                                        NULL,
    parallelism    int                                        NOT NULL,
    max_bulk_size  int                                        NOT NULL,
    version        VARCHAR(128)                               NOT NULL,
    partitioned    BOOLEAN                                    NOT NULL DEFAULT TRUE,
    created_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP        NULL,
    updated_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP        NULL ON UPDATE CURRENT_TIMESTAMP,
    description    VARCHAR(512)                               NULL,
    UNIQUE (area, vertical, name, version)
);

create table if not exists TARGET
(
    id            BIGINT PRIMARY KEY,
    table_id      BIGINT                              NOT NULL,
    connection_id BIGINT                              NULL,
    format        VARCHAR(64)                         NOT NULL,
    created_ts    TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts    TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
    index (table_id),
    index (connection_id),
    UNIQUE (table_id, connection_id, format)
);

create table if not exists DEPENDENCY
(
    id                       BIGINT PRIMARY KEY,
    table_id                 BIGINT                                     NOT NULL,
    dependency_target_id     BIGINT                                     NOT NULL,
    dependency_table_id      BIGINT                                     NOT NULL,
    transform_type           enum ('NONE', 'AGGREGATE', 'WINDOW', 'IDENTITY', 'TEMPORAL_AGGREGATE') NOT NULL,
    transform_partition_unit enum ('WEEKS', 'DAYS', 'HOURS', 'MINUTES') NULL,
    transform_partition_size int       DEFAULT 1,
    created_ts               TIMESTAMP DEFAULT CURRENT_TIMESTAMP        NULL,
    updated_ts               TIMESTAMP DEFAULT CURRENT_TIMESTAMP        NULL ON UPDATE CURRENT_TIMESTAMP,
    description              VARCHAR(512)                               NULL,
    index (table_id),
    index (dependency_target_id)
);

create table if not exists PARTITION_
(
    id           BIGINT PRIMARY KEY,
    target_id    BIGINT                              NOT NULL,
    partition_ts TIMESTAMP                           NOT NULL,
    partitioned  BOOLEAN                             NOT NULL DEFAULT TRUE,
    created_ts   TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts   TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
    index (target_id),
    index (partition_ts)
);

create table if not exists PARTITION_DEPENDENCY
(
    id                      BIGINT PRIMARY KEY,
    partition_id            BIGINT                              NOT NULL,
    dependency_partition_id BIGINT                              NOT NULL,
    created_ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
    index (partition_id),
    index (dependency_partition_id)
);

CREATE TABLE IF NOT EXISTS SOURCE
(
    id                  BIGINT PRIMARY KEY,
    table_id            BIGINT                                     NOT NULL,
    connection_id       BIGINT                                     NOT NULL,
    path                VARCHAR(512)                               NOT NULL,
    format              VARCHAR(64)                                NOT NULL,
    partition_unit      enum ('WEEKS', 'DAYS', 'HOURS', 'MINUTES') NULL,
    partition_size      int                                        NULL,
    sql_select_query    VARCHAR(2048) DEFAULT NULL,
    sql_partition_query VARCHAR(2048) DEFAULT NULL,
    partition_column    VARCHAR(256)  DEFAULT NULL,
    partition_num       INT                                        NOT NULL,
    query_column        VARCHAR(256)  DEFAULT NULL,
    partitioned         BOOLEAN       NOT NULL DEFAULT TRUE,
    created_ts          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_ts          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    description         VARCHAR(512),
    index (table_id),
    index (connection_id)
);