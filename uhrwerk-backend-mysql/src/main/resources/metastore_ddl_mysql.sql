USE UHRWERK_METASTORE;
CREATE TABLE IF NOT EXISTS CONNECTION
(
    id                    BIGINT AUTO_INCREMENT PRIMARY KEY,
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
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    area           VARCHAR(128)                               NOT NULL,
    vertical       VARCHAR(128)                               NOT NULL,
    name           VARCHAR(128)                               NOT NULL,
    partition_unit enum ('WEEKS', 'DAYS', 'HOURS', 'MINUTES') NOT NULL,
    partition_size int                                        NOT NULL,
    parallelism    int                                        NOT NULL,
    max_bulk_size  int                                        NOT NULL,
    version        VARCHAR(128)                               NOT NULL,
    created_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP        NULL,
    updated_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP        NULL ON UPDATE CURRENT_TIMESTAMP,
    description    VARCHAR(512)                               NULL,
    UNIQUE (area, vertical, name, version)
);

create table if not exists TARGET
(
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id      BIGINT                              NOT NULL,
    connection_id BIGINT                              NULL,
    format        VARCHAR(64)                         NOT NULL,
    created_ts    TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts    TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (table_id) REFERENCES TABLE_ (id) ON DELETE CASCADE,
    FOREIGN KEY (connection_id) REFERENCES CONNECTION (id) ON DELETE RESTRICT,
    UNIQUE (table_id, connection_id, format)
);

create table if not exists DEPENDENCY
(
    id                       BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id                 BIGINT                                     NOT NULL,
    target_id                BIGINT                                     NOT NULL,
    transform_type           enum ('AGGREGATE', 'WINDOW', 'IDENTITY')   NOT NULL,
    transform_partition_unit enum ('WEEKS', 'DAYS', 'HOURS', 'MINUTES') NULL,
    transform_partition_size int                                        DEFAULT 1,
    created_ts               TIMESTAMP DEFAULT CURRENT_TIMESTAMP        NULL,
    updated_ts               TIMESTAMP DEFAULT CURRENT_TIMESTAMP        NULL ON UPDATE CURRENT_TIMESTAMP,
    description              VARCHAR(512)                               NULL,
    FOREIGN KEY (table_id) REFERENCES TABLE_ (id) ON DELETE CASCADE,
    FOREIGN KEY (target_id) REFERENCES TARGET (id) ON DELETE RESTRICT
);

create table if not exists PARTITION_
(
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    target_id    BIGINT                              NOT NULL,
    partition_ts TIMESTAMP                           NOT NULL,
    year         VARCHAR(32)                         NOT NULL,
    month        VARCHAR(32)                         NOT NULL,
    day          VARCHAR(32)                         NOT NULL,
    hour         VARCHAR(32)                         NOT NULL,
    minute       VARCHAR(32)                         NOT NULL,
    created_ts   TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts   TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (target_id) REFERENCES TARGET (id)
        ON DELETE CASCADE,
    INDEX (year, month, day, hour, minute),
    index (partition_ts)
);

create table if not exists PARTITION_DEPENDENCY
(
    id                      BIGINT AUTO_INCREMENT PRIMARY KEY,
    partition_id            BIGINT                              NOT NULL,
    dependency_partition_id BIGINT                              NOT NULL,
    created_ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (partition_id) REFERENCES PARTITION_ (id)
        ON DELETE CASCADE,
    FOREIGN KEY (dependency_partition_id) REFERENCES PARTITION_ (id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS SOURCE
(
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id            BIGINT                                     NOT NULL,
    connection_id       BIGINT                                     NOT NULL,
    path                VARCHAR(512)                               NOT NULL,
    partition_unit      enum ('WEEKS', 'DAYS', 'HOURS', 'MINUTES') NOT NULL,
    partition_size      int                                        NOT NULL,
    sql_select_query    VARCHAR(2048) DEFAULT NULL,
    sql_partition_query VARCHAR(2048) DEFAULT NULL,
    partition_column    VARCHAR(256)  DEFAULT NULL,
    partition_num       INT                                        NOT NULL,
    query_column        VARCHAR(256)  DEFAULT NULL,
    created_ts          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_ts          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    description         VARCHAR(512),
    FOREIGN KEY (table_id)
        REFERENCES TABLE_ (id) ON DELETE CASCADE,
    FOREIGN KEY (connection_id)
        REFERENCES CONNECTION (id) ON DELETE RESTRICT
);
