USE UHRWERK_METASTORE;
CREATE TABLE IF NOT EXISTS CONNECTION
(
    id                    BIGINT AUTO_INCREMENT PRIMARY KEY,
    name                  VARCHAR(256)                           NOT NULL,
    type                  enum ('FS', 'JDBC', 'S3', 'GC', 'ABS') NOT NULL,
    path                  VARCHAR(512)                           NOT NULL,
    jdbc_url              VARCHAR(512)                           NOT NULL,
    jdbc_driver           VARCHAR(512)                           NOT NULL,
    jdbc_user             VARCHAR(512)                           NOT NULL,
    jdbc_pass             VARCHAR(512)                           NOT NULL,
    aws_access_key_id     VARCHAR(512)                           NOT NULL,
    aws_secret_access_key VARCHAR(512)                           NOT NULL,
    version               VARCHAR(256)                           NOT NULL,
    created_ts            TIMESTAMP DEFAULT CURRENT_TIMESTAMP    NULL,
    updated_ts            TIMESTAMP DEFAULT CURRENT_TIMESTAMP    NULL ON UPDATE CURRENT_TIMESTAMP,
    description           VARCHAR(512)                           NULL
);

create table if not exists TABLE_
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

CREATE INDEX TABLE_INDX ON TABLE_ (area(128), vertical(128), table_name(128), version(64));

create table if not exists TARGET
(
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id      BIGINT                              NOT NULL,
    connection_id BIGINT                              NOT NULL,
    format        VARCHAR(64)                         NOT NULL,
    created_ts    TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts    TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (table_id) REFERENCES TABLE_ (id),
    FOREIGN KEY (connection_id) REFERENCES CONNECTION (id)
);

create table if not exists DEPENDENCY
(
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id            BIGINT                                                        NOT NULL,
    target_id           BIGINT                                                        NOT NULL,
    partition_transform enum ('AGGREGATE', 'WINDOW', 'ONEONONE')                      NOT NULL,
    batch_temporal_unit enum ('YEARS', 'MONTHS', 'WEEKS', 'DAYS', 'HOURS', 'MINUTES') NOT NULL,
    batch_size          int                                                           NOT NULL,
    created_ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP                           NULL,
    updated_ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP                           NULL ON UPDATE CURRENT_TIMESTAMP,
    description         VARCHAR(512)                                                  NULL,
    FOREIGN KEY (target_id) REFERENCES TARGET (id),
    FOREIGN KEY (table_id) REFERENCES TABLE_ (id)
);

create table if not exists PARTITION_
(
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    target_id      BIGINT                              NOT NULL,
    path           VARCHAR(512)                        NOT NULL,
    year           VARCHAR(32)                         NOT NULL,
    month          VARCHAR(32)                         NOT NULL,
    day            VARCHAR(32)                         NOT NULL,
    hour           VARCHAR(32)                         NOT NULL,
    minute         VARCHAR(32)                         NOT NULL,
    partition_hash VARCHAR(128)                        NOT NULL,
    created_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (target_id) REFERENCES TARGET (id)
        ON DELETE CASCADE
);

create table if not exists PARTITION_DEPENDENCY
(
    id                      BIGINT AUTO_INCREMENT PRIMARY KEY,
    partition_id            BIGINT                              NOT NULL,
    dependency_partition_id BIGINT                              NOT NULL,
    created_ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (partition_id) REFERENCES PARTITION_ (id)
        on delete cascade,
    FOREIGN KEY (dependency_partition_id) REFERENCES PARTITION_ (id)
        on delete cascade
);

CREATE TABLE IF NOT EXISTS SOURCE
(
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id            BIGINT       NOT NULL,
    connection_id       BIGINT       NOT NULL,
    path                VARCHAR(512) NOT NULL,
    sql_select_query    VARCHAR(2048) DEFAULT NULL,
    sql_partition_query VARCHAR(2048) DEFAULT NULL,
    partition_column    VARCHAR(256)  DEFAULT NULL,
    query_column        VARCHAR(256)  DEFAULT NULL,
    created_ts          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_ts          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    description         VARCHAR(512),
    FOREIGN KEY (table_id)
        REFERENCES TABLE_ (id),
    FOREIGN KEY (connection_id)
        REFERENCES CONNECTION (id)
);
