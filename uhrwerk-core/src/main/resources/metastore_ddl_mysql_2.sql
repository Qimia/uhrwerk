-- DROP TABLE IF EXISTS UHRWERK_METASTORE.DAG_TABLE_SPEC;
CREATE TABLE IF NOT EXISTS UHRWERK_METASTORE.DAG_TABLE_SPEC
(
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    area        VARCHAR(256) NOT NULL,
    vertical    VARCHAR(256) NOT NULL,
    table_name  VARCHAR(256) NOT NULL,
    version     VARCHAR(256) NOT NULL,
    created_ts  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_ts  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    external    BOOLEAN   DEFAULT 0,
    description VARCHAR(512),
    INDEX idx_table (area, vertical, table_name)
) ENGINE = INNODB
  DEFAULT CHARSET = UTF8MB4;

-- DROP TABLE IF EXISTS UHRWERK_METASTORE.DAG_TABLE_SPEC;
CREATE TABLE IF NOT EXISTS UHRWERK_METASTORE.DAG_PARTITION_SPEC
(
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_info_id       BIGINT                                                   NOT NULL,
    batch_temporal_unit ENUM ('YEARS','MONTHS','WEEKS','DAYS','HOURS','MINUTES') NOT NULL,
    batch_size          INT                                                      NOT NULL,
    version             VARCHAR(256)                                             NOT NULL,
    created_ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    description         VARCHAR(512),
    FOREIGN KEY (table_info_id)
        REFERENCES UHRWERK_METASTORE.DAG_TABLE_SPEC (id)
        ON DELETE CASCADE
) ENGINE = INNODB
  DEFAULT CHARSET = UTF8MB4;

-- DROP TABLE IF EXISTS UHRWERK_METASTORE.CONNECTION_CONFIG;
CREATE TABLE IF NOT EXISTS UHRWERK_METASTORE.DAG_CONNECTION
(
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    connection_name VARCHAR(256) NOT NULL,
    connection_type VARCHAR(256) NOT NULL,
    connection_url  VARCHAR(512) NOT NULL,
    version         VARCHAR(256) NOT NULL,
    created_ts      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_ts      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    description     VARCHAR(512)
) ENGINE = INNODB
  DEFAULT CHARSET = UTF8MB4;

-- DROP TABLE IF EXISTS UHRWERK_METASTORE.STEP_CONFIG;
CREATE TABLE IF NOT EXISTS UHRWERK_METASTORE.DAG_STEP
(
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id       BIGINT       NOT NULL,
    parallelism    INT          NOT NULL,
    max_partitions INT          NOT NULL,
    version        VARCHAR(256) NOT NULL,
    created_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    description    VARCHAR(512),
    FOREIGN KEY (table_id)
        REFERENCES UHRWERK_METASTORE.DAG_TABLE_SPEC (id)
) ENGINE = INNODB
  DEFAULT CHARSET = UTF8MB4;

CREATE TABLE IF NOT EXISTS UHRWERK_METASTORE.DT_TABLE
(
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_spec_id BIGINT       NOT NULL,
    connection_id BIGINT       NOT NULL,
    path          VARCHAR(512) NOT NULL,
    version       VARCHAR(256) NOT NULL,
    created_ts    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_ts    TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    external      BOOLEAN   DEFAULT 0,
    description   VARCHAR(512),
    FOREIGN KEY (table_spec_id)
        REFERENCES UHRWERK_METASTORE.DAG_TABLE_SPEC (id),
    FOREIGN KEY (connection_id)
        REFERENCES UHRWERK_METASTORE.DAG_CONNECTION (id)
) ENGINE = INNODB
  DEFAULT CHARSET = UTF8MB4;

-- DROP TABLE IF EXISTS UHRWERK_METASTORE.DAG_TARGET;
CREATE TABLE IF NOT EXISTS UHRWERK_METASTORE.DAG_TARGET
(
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    step_id     BIGINT       NOT NULL,
    table_id    BIGINT       NOT NULL,
    version     VARCHAR(256) NOT NULL,
    created_ts  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_ts  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    description VARCHAR(512),
    FOREIGN KEY (table_id)
        REFERENCES UHRWERK_METASTORE.DT_TABLE (id),
    FOREIGN KEY (step_id)
        REFERENCES UHRWERK_METASTORE.DAG_STEP (id)
) ENGINE = INNODB
  DEFAULT CHARSET = UTF8MB4;

-- DROP TABLE IF EXISTS UHRWERK_METASTORE.DAG_DEPENDENCY;
CREATE TABLE IF NOT EXISTS UHRWERK_METASTORE.DAG_DEPENDENCY
(
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id            BIGINT                                                   NOT NULL,
    step_id             BIGINT                                                   NOT NULL,
    partition_transform ENUM ('AGGREGATE','WINDOW')                              NOT NULL,
    batch_temporal_unit ENUM ('YEARS','MONTHS','WEEKS','DAYS','HOURS','MINUTES') NOT NULL,
    batch_size          INT                                                      NOT NULL,
    version             VARCHAR(256)                                             NOT NULL,
    created_ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    description         VARCHAR(512),
    FOREIGN KEY (table_id)
        REFERENCES UHRWERK_METASTORE.DT_TABLE (id),
    FOREIGN KEY (step_id)
        REFERENCES UHRWERK_METASTORE.DAG_STEP (id)
) ENGINE = INNODB
  DEFAULT CHARSET = UTF8MB4;


-- DROP TABLE IF EXISTS UHRWERK_METASTORE.PARTITION_LOG;
CREATE TABLE IF NOT EXISTS UHRWERK_METASTORE.DT_PARTITION
(
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id       BIGINT       NOT NULL,
    task_id        BIGINT       NOT NULL,
    path           VARCHAR(512) NOT NULL,
    year           VARCHAR(32)  NOT NULL,
    month          VARCHAR(32)  NOT NULL,
    day            VARCHAR(32)  NOT NULL,
    hour           VARCHAR(32)  NOT NULL,
    minute         VARCHAR(32)  NOT NULL,
    partition_hash VARCHAR(128) NOT NULL,
    created_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    description    VARCHAR(512),
    FOREIGN KEY (table_id)
        REFERENCES UHRWERK_METASTORE.DT_TABLE (id)
        ON DELETE CASCADE
) ENGINE = INNODB
  DEFAULT CHARSET = UTF8MB4;

-- DROP TABLE IF EXISTS UHRWERK_METASTORE.PARTITION_LOG;
CREATE TABLE IF NOT EXISTS UHRWERK_METASTORE.DT_PARTITION_DEPENDENCY
(
    id                      BIGINT AUTO_INCREMENT PRIMARY KEY,
    partition_id            BIGINT NOT NULL,
    dependency_partition_id BIGINT NOT NULL,
    created_ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    description             VARCHAR(512),
    FOREIGN KEY (partition_id)
        REFERENCES UHRWERK_METASTORE.DT_PARTITION (id)
        ON DELETE CASCADE,
    FOREIGN KEY (dependency_partition_id)
        REFERENCES UHRWERK_METASTORE.DT_PARTITION (id)
        ON DELETE CASCADE
) ENGINE = INNODB
  DEFAULT CHARSET = UTF8MB4;

-- DROP TABLE IF EXISTS UHRWERK_METASTORE.DAG_EXT_SOURCE;
CREATE TABLE IF NOT EXISTS UHRWERK_METASTORE.DAG_EXT_SOURCE
(
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    step_id             BIGINT                                                   NOT NULL,
    source_type         ENUM ('jdbc','raw')                                      NOT NULL,
    connection_id       BIGINT                                                   NOT NULL,
    batch_temporal_unit ENUM ('YEARS','MONTHS','WEEKS','DAYS','HOURS','MINUTES') NOT NULL,
    batch_size          INT                                                      NOT NULL,
    sql_select_query    VARCHAR(2048) DEFAULT NULL,
    sql_partition_query VARCHAR(2048) DEFAULT NULL,
    partition_column    VARCHAR(256)  DEFAULT NULL,
    query_column        VARCHAR(256)  DEFAULT NULL,
    version             VARCHAR(256)                                             NOT NULL,
    created_ts          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_ts          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    description         VARCHAR(512),
    FOREIGN KEY (step_id)
        REFERENCES UHRWERK_METASTORE.DAG_STEP (id),
    FOREIGN KEY (connection_id)
        REFERENCES UHRWERK_METASTORE.DAG_CONNECTION (id)
) ENGINE = INNODB
  DEFAULT CHARSET = UTF8MB4;