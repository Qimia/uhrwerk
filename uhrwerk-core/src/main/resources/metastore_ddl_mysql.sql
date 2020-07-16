-- DROP TABLE IF EXISTS UHRWERK_METASTORE.TABLE_INFO;
CREATE TABLE IF NOT EXISTS UHRWERK_METASTORE.TABLE_INFO
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

-- DROP TABLE IF EXISTS UHRWERK_METASTORE.TABLE_PARTITION_INFO;
CREATE TABLE IF NOT EXISTS UHRWERK_METASTORE.TABLE_PARTITION_INFO
(
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_info_id       BIGINT                                                   NOT NULL,
    batch_temporal_unit ENUM ('YEARS','MONTHS','WEEKS','DAYS','HOURS','MINUTES') NOT NULL,
    batch_size          INT                                                      NOT NULL,
    partition_column    VARCHAR(256)                                             NOT NULL,
    version             VARCHAR(256)                                             NOT NULL,
    created_ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    description         VARCHAR(512),
    FOREIGN KEY (table_info_id)
        REFERENCES UHRWERK_METASTORE.TABLE_INFO (id)
        ON DELETE CASCADE
) ENGINE = INNODB
  DEFAULT CHARSET = UTF8MB4;

-- DROP TABLE IF EXISTS UHRWERK_METASTORE.CONNECTION_CONFIG;
CREATE TABLE IF NOT EXISTS UHRWERK_METASTORE.CONNECTION_CONFIG
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

-- DROP TABLE IF EXISTS UHRWERK_METASTORE.DEPENDENCY_CONFIG;
CREATE TABLE IF NOT EXISTS UHRWERK_METASTORE.DEPENDENCY_CONFIG
(
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id            BIGINT                                                   NOT NULL,
    dependency_table_id BIGINT                                                   NOT NULL,
    partition_transform ENUM ('AGGREGATE','WINDOW','SPLIT')                      NOT NULL,
    partition_column    VARCHAR(256)                                             NOT NULL,
    temporal_unit       ENUM ('YEARS','MONTHS','WEEKS','DAYS','HOURS','MINUTES') NOT NULL,
    batch_size          INT                                                      NOT NULL,
    version             VARCHAR(256)                                             NOT NULL,
    created_ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    description         VARCHAR(512),
    FOREIGN KEY (table_id)
        REFERENCES UHRWERK_METASTORE.TABLE_INFO (id),
    FOREIGN KEY (dependency_table_id)
        REFERENCES UHRWERK_METASTORE.TABLE_INFO (id)
) ENGINE = INNODB
  DEFAULT CHARSET = UTF8MB4;

-- DROP TABLE IF EXISTS UHRWERK_METASTORE.STEP_CONFIG;
CREATE TABLE IF NOT EXISTS UHRWERK_METASTORE.STEP_CONFIG
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
        REFERENCES UHRWERK_METASTORE.TABLE_INFO (id)
) ENGINE = INNODB
  DEFAULT CHARSET = UTF8MB4;

-- DROP TABLE IF EXISTS UHRWERK_METASTORE.TARGET_CONFIG;
CREATE TABLE IF NOT EXISTS UHRWERK_METASTORE.TARGET_CONFIG
(
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id       BIGINT       NOT NULL,
    step_config_id BIGINT       NOT NULL,
    version        VARCHAR(256) NOT NULL,
    created_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    description    VARCHAR(512),
    FOREIGN KEY (table_id)
        REFERENCES UHRWERK_METASTORE.TABLE_INFO (id),
    FOREIGN KEY (step_config_id)
        REFERENCES UHRWERK_METASTORE.STEP_CONFIG (id)
) ENGINE = INNODB
  DEFAULT CHARSET = UTF8MB4;

-- DROP TABLE IF EXISTS UHRWERK_METASTORE.PARTITION_LOG;
CREATE TABLE IF NOT EXISTS UHRWERK_METASTORE.PARTITION_LOG
(
    id                BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id          BIGINT       NOT NULL,
    task_id           BIGINT       NOT NULL,
    partition_info_id BIGINT       NOT NULL,
    connection_id     BIGINT       NOT NULL,
    path              VARCHAR(512) NOT NULL,
    year              VARCHAR(32)  NOT NULL,
    month             VARCHAR(32)  NOT NULL,
    day               VARCHAR(32)  NOT NULL,
    hour              VARCHAR(32)  NOT NULL,
    minute            VARCHAR(32)  NOT NULL,
    partition_hash    VARCHAR(128) NOT NULL,
    created_ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    description       VARCHAR(512),
    FOREIGN KEY (table_id)
        REFERENCES UHRWERK_METASTORE.TABLE_INFO (id)
        ON DELETE CASCADE,
    FOREIGN KEY (partition_info_id)
        REFERENCES UHRWERK_METASTORE.TABLE_PARTITION_INFO (id)
        ON DELETE CASCADE,
    FOREIGN KEY (connection_id)
        REFERENCES UHRWERK_METASTORE.CONNECTION_CONFIG (id)
        ON DELETE CASCADE
) ENGINE = INNODB
  DEFAULT CHARSET = UTF8MB4;