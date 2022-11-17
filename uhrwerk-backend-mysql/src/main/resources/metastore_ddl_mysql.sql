USE UHRWERK_METASTORE;
CREATE TABLE IF NOT EXISTS CONNECTION
(
    id                    BIGINT AUTO_INCREMENT PRIMARY KEY,
    name                  VARCHAR(256)                           NOT NULL,
    type                  enum ('FS', 'JDBC', 'S3', 'GC', 'ABS') NOT NULL,
    path                  VARCHAR(512)                           NULL,
    jdbc_url              VARCHAR(512)                           NULL,
    jdbc_driver           VARCHAR(512)                           NULL,
    jdbc_user             VARCHAR(512)                           NULL,
    jdbc_pass             VARCHAR(512)                           NULL,
    aws_access_key_id     VARCHAR(512)                           NULL,
    aws_secret_access_key VARCHAR(512)                           NULL,
    deactivated_ts        TIMESTAMP                              NULL,
    created_ts            TIMESTAMP DEFAULT CURRENT_TIMESTAMP    NULL,
    updated_ts            TIMESTAMP DEFAULT CURRENT_TIMESTAMP    NULL ON update CURRENT_TIMESTAMP,
    description           VARCHAR(512)                           NULL,
    hash_key              BIGINT                                 NOT NULL,
    INDEX (hash_key),
    UNIQUE (name, deactivated_ts)
);

CREATE TABLE IF NOT EXISTS SECRET_
(
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    name            VARCHAR(128)                        NOT NULL,
    type            enum ('AWS','AZURE','GCP')          NOT NULL,
    aws_secret_name VARCHAR(512)                        NULL,
    aws_region      VARCHAR(32)                         NULL,
    deactivated_ts  TIMESTAMP                           NULL,
    created_ts      TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts      TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON update CURRENT_TIMESTAMP,
    description     VARCHAR(512)                        NULL,
    hash_key        BIGINT                              NOT NULL,
    INDEX (hash_key),
    UNIQUE (name, deactivated_ts)
);


CREATE TABLE IF NOT EXISTS TABLE_
(
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    area           VARCHAR(128)                               NOT NULL,
    vertical       VARCHAR(128)                               NOT NULL,
    name           VARCHAR(128)                               NOT NULL,
    version        VARCHAR(128)                               NOT NULL,
    partition_unit enum ('WEEKS', 'DAYS', 'HOURS', 'MINUTES') NULL,
    partition_size int                                        NOT NULL,
    parallelism    int                                        NOT NULL,
    max_bulk_size  int                                        NOT NULL,
    class_name     VARCHAR(128)                               NOT NULL,
    partitioned    BOOLEAN                                    NOT NULL DEFAULT FALSE,
    deactivated_ts TIMESTAMP                                  NULL,
    created_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP        NULL,
    updated_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP        NULL ON update CURRENT_TIMESTAMP,
    description    VARCHAR(512)                               NULL,
    hash_key       BIGINT                                     NOT NULL,
    INDEX (hash_key),
    UNIQUE (area, vertical, name, version, deactivated_ts)
);

CREATE TABLE IF NOT EXISTS SOURCE
(
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id            BIGINT                                     NOT NULL,
    connection_id       BIGINT                                     NOT NULL,
    path                VARCHAR(512)                               NOT NULL,
    format              VARCHAR(64)                                NOT NULL,
    partition_unit      enum ('WEEKS', 'DAYS', 'HOURS', 'MINUTES') NULL,
    partition_size      int                                        NULL,
    sql_select_query    VARCHAR(2048)                                       DEFAULT NULL,
    sql_partition_query VARCHAR(2048)                                       DEFAULT NULL,
    partition_column    VARCHAR(256)                                        DEFAULT NULL,
    partition_num       INT                                        NOT NULL,
    query_column        VARCHAR(256)                                        DEFAULT NULL,
    partitioned         BOOLEAN                                    NOT NULL DEFAULT TRUE,
    auto_load           BOOLEAN                                    NOT NULL,
    deactivated_ts      TIMESTAMP                                  NULL,
    created_ts          TIMESTAMP                                           DEFAULT CURRENT_TIMESTAMP,
    updated_ts          TIMESTAMP                                           DEFAULT CURRENT_TIMESTAMP ON update CURRENT_TIMESTAMP,
    description         VARCHAR(512),
    hash_key            BIGINT                                     NOT NULL,
    INDEX (hash_key),
    UNIQUE (table_id, connection_id, path, format, deactivated_ts),
    FOREIGN KEY (table_id) REFERENCES TABLE_ (id) ON DELETE CASCADE,
    FOREIGN KEY (connection_id) REFERENCES CONNECTION (id) ON DELETE CASCADE

);

CREATE TABLE IF NOT EXISTS TARGET
(
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id       BIGINT                              NOT NULL,
    connection_id  BIGINT                              NOT NULL,
    format         VARCHAR(64)                         NOT NULL,
    deactivated_ts TIMESTAMP                           NULL,
    created_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON update CURRENT_TIMESTAMP,
    hash_key       BIGINT                              NOT NULL,
    INDEX (hash_key),
    UNIQUE (table_id, connection_id, format, deactivated_ts),
    FOREIGN KEY (table_id) REFERENCES TABLE_ (id) ON DELETE CASCADE,
    FOREIGN KEY (connection_id) REFERENCES CONNECTION (id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS DEPENDENCY
(
    id                       BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id                 BIGINT                                           NOT NULL,
    dependency_target_id     BIGINT                                           NOT NULL,
    dependency_table_id      BIGINT                                           NOT NULL,
    transform_type           enum ('NONE', 'AGGREGATE', 'WINDOW', 'IDENTITY') NOT NULL,
    transform_partition_unit enum ('WEEKS', 'DAYS', 'HOURS', 'MINUTES')       NULL,
    transform_partition_size int       DEFAULT 1,
    deactivated_ts           TIMESTAMP                                        NULL,
    created_ts               TIMESTAMP DEFAULT CURRENT_TIMESTAMP              NULL,
    updated_ts               TIMESTAMP DEFAULT CURRENT_TIMESTAMP              NULL ON update CURRENT_TIMESTAMP,
    description              VARCHAR(512)                                     NULL,
    hash_key                 BIGINT                                           NOT NULL,
    INDEX (hash_key),
    UNIQUE (table_id, dependency_target_id, dependency_table_id, deactivated_ts),
    FOREIGN KEY (table_id) REFERENCES TABLE_ (id) ON DELETE CASCADE,
    FOREIGN KEY (dependency_target_id) REFERENCES TARGET (id) ON DELETE CASCADE,
    FOREIGN KEY (dependency_table_id) REFERENCES TABLE_ (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS PARTITION_
(
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    target_id    BIGINT       NOT NULL,
    partition_ts TIMESTAMP    NOT NULL,
    partitioned  BOOLEAN      NOT NULL DEFAULT FALSE,
    bookmarked   BOOLEAN      NOT NULL DEFAULT FALSE,
    max_bookmark VARCHAR(128) NULL,
    created_ts   TIMESTAMP             DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts   TIMESTAMP             DEFAULT CURRENT_TIMESTAMP NULL ON update CURRENT_TIMESTAMP,
    INDEX (partition_ts),
    INDEX (max_bookmark),
    UNIQUE (target_id, partition_ts),
    FOREIGN KEY (target_id) REFERENCES TARGET (id) ON DELETE CASCADE
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