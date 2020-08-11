USE UHRWERK_METASTORE;
CREATE TABLE IF NOT EXISTS CF_CONNECTION
(
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    connection_name VARCHAR(256)                        NOT NULL,
    connection_type VARCHAR(256)                        NOT NULL,
    connection_url  VARCHAR(512)                        NOT NULL,
    version         VARCHAR(256)                        NOT NULL,
    created_ts      TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts      TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
    description     VARCHAR(512)                        NULL
);

create table if not exists CF_TABLE
(
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    area                VARCHAR(256)                                                  NOT NULL,
    vertical            VARCHAR(256)                                                  NOT NULL,
    table_name          VARCHAR(256)                                                  NOT NULL,
    batch_temporal_unit enum ('YEARS', 'MONTHS', 'WEEKS', 'DAYS', 'HOURS', 'MINUTES') NOT NULL,
    batch_size          int                                                           NOT NULL,
    parallelism         int                                                           NOT NULL,
    max_partitions      int                                                           NOT NULL,
    version             VARCHAR(256)                                                  NOT NULL,
    created_ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP                           NULL,
    updated_ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP                           NULL ON UPDATE CURRENT_TIMESTAMP,
    description         VARCHAR(512)                                                  NULL
);

CREATE INDEX TABLE_INDX ON CF_TABLE (area(128), vertical(128), table_name(128), version(64));

create table if not exists DT_TARGET
(
    id               BIGINT AUTO_INCREMENT PRIMARY KEY,
    cf_table_id      BIGINT                              NOT NULL,
    cf_connection_id BIGINT                              NOT NULL,
    format           VARCHAR(64)                         NOT NULL,
    created_ts       TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts       TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
    description      VARCHAR(512)                        NULL,
    FOREIGN KEY (cf_table_id) REFERENCES CF_TABLE (id),
    FOREIGN KEY (cf_connection_id) REFERENCES CF_CONNECTION (id)
);

create table if not exists DT_DEPENDENCY
(
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    cf_table_id         BIGINT                                                        NOT NULL,
    dt_target_id        BIGINT                                                        NOT NULL,
    partition_transform enum ('AGGREGATE', 'WINDOW', 'ONEONONE')                      NOT NULL,
    batch_temporal_unit enum ('YEARS', 'MONTHS', 'WEEKS', 'DAYS', 'HOURS', 'MINUTES') NOT NULL,
    batch_size          int                                                           NOT NULL,
    created_ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP                           NULL,
    updated_ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP                           NULL ON UPDATE CURRENT_TIMESTAMP,
    description         VARCHAR(512)                                                  NULL,
    FOREIGN KEY (dt_target_id) REFERENCES DT_TARGET (id),
    FOREIGN KEY (cf_table_id) REFERENCES CF_TABLE (id)
);

create table if not exists DT_PARTITION
(
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    dt_target_id   BIGINT                              NOT NULL,
    path           VARCHAR(512)                        NOT NULL,
    year           VARCHAR(32)                         NOT NULL,
    month          VARCHAR(32)                         NOT NULL,
    day            VARCHAR(32)                         NOT NULL,
    hour           VARCHAR(32)                         NOT NULL,
    minute         VARCHAR(32)                         NOT NULL,
    partition_hash VARCHAR(128)                        NOT NULL,
    created_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (dt_target_id) REFERENCES DT_TARGET (id)
        ON DELETE CASCADE
);

create table if not exists DT_PARTITION_DEPENDENCY
(
    id                      BIGINT AUTO_INCREMENT PRIMARY KEY,
    partition_id            BIGINT                              NOT NULL,
    dependency_partition_id BIGINT                              NOT NULL,
    created_ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (partition_id) REFERENCES DT_PARTITION (id)
        on delete cascade,
    FOREIGN KEY (dependency_partition_id) REFERENCES DT_PARTITION (id)
        on delete cascade
);

CREATE TABLE IF NOT EXISTS DT_SOURCE
(
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    cf_table_id         BIGINT       NOT NULL,
    connection_id       BIGINT       NOT NULL,
    path                VARCHAR(512) NOT NULL,
    sql_select_query    VARCHAR(2048) DEFAULT NULL,
    sql_partition_query VARCHAR(2048) DEFAULT NULL,
    partition_column    VARCHAR(256)  DEFAULT NULL,
    query_column        VARCHAR(256)  DEFAULT NULL,
    created_ts          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_ts          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    description         VARCHAR(512),
    FOREIGN KEY (cf_table_id)
        REFERENCES CF_TABLE (id),
    FOREIGN KEY (connection_id)
        REFERENCES CF_CONNECTION (id)
);
