use UHRWERK_METASTORE;
create table if not exists CF_CONNECTION
(
    id              bigint auto_increment primary key,
    connection_name varchar(256)                        not null,
    connection_type varchar(256)                        not null,
    connection_url  varchar(512)                        not null,
    version         varchar(256)                        not null,
    created_ts      timestamp default CURRENT_TIMESTAMP null,
    updated_ts      timestamp default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP,
    description     varchar(512)                        null
);

create table if not exists CF_TABLE
(
    id                  bigint auto_increment primary key,
    area                varchar(256)                                                  not null,
    vertical            varchar(256)                                                  not null,
    table_name          varchar(256)                                                  not null,
    batch_temporal_unit enum ('YEARS', 'MONTHS', 'WEEKS', 'DAYS', 'HOURS', 'MINUTES') not null,
    batch_size          int                                                           not null,
    parallelism         int                                                           not null,
    max_partitions      int                                                           not null,
    version             varchar(256)                                                  not null,
    created_ts          timestamp default CURRENT_TIMESTAMP                           null,
    updated_ts          timestamp default CURRENT_TIMESTAMP                           null on update CURRENT_TIMESTAMP,
    description         varchar(512)                                                  null
);

create table if not exists DT_TABLE
(
    id               bigint auto_increment primary key,
    cf_table_id      bigint                              not null,
    cf_connection_id bigint                              not null,
    path             varchar(512)                        not null,
    version          varchar(256)                        not null,
    created_ts       timestamp default CURRENT_TIMESTAMP null,
    updated_ts       timestamp default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP,
    description      varchar(512)                        null,
    FOREIGN KEY (cf_table_id) references CF_TABLE (id),
    FOREIGN KEY (cf_connection_id) references CF_CONNECTION (id)
);

create table if not exists DT_DEPENDENCY
(
    id                  bigint auto_increment primary key,
    cf_table_id         bigint                                                        not null,
    dt_table_id         bigint                                                        not null,
    partition_transform enum ('AGGREGATE', 'WINDOW')                                  not null,
    batch_temporal_unit enum ('YEARS', 'MONTHS', 'WEEKS', 'DAYS', 'HOURS', 'MINUTES') not null,
    batch_size          int                                                           not null,
    version             varchar(256)                                                  not null,
    created_ts          timestamp default CURRENT_TIMESTAMP                           null,
    updated_ts          timestamp default CURRENT_TIMESTAMP                           null on update CURRENT_TIMESTAMP,
    description         varchar(512)                                                  null,
    FOREIGN KEY (dt_table_id) references DT_TABLE (id),
    FOREIGN KEY (cf_table_id) REFERENCES CF_TABLE (id)
);

create table if not exists DT_PARTITION
(
    id             bigint auto_increment primary key,
    dt_table_id    bigint                              not null,
    path           varchar(512)                        not null,
    year           varchar(32)                         not null,
    month          varchar(32)                         not null,
    day            varchar(32)                         not null,
    hour           varchar(32)                         not null,
    minute         varchar(32)                         not null,
    partition_hash varchar(128)                        not null,
    created_ts     timestamp default CURRENT_TIMESTAMP null,
    updated_ts     timestamp default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP,
    FOREIGN KEY (dt_table_id) REFERENCES DT_TABLE (id)
        ON DELETE CASCADE
);

create table if not exists DT_PARTITION_DEPENDENCY
(
    id                      bigint auto_increment primary key,
    partition_id            bigint                              not null,
    dependency_partition_id bigint                              not null,
    created_ts              timestamp default CURRENT_TIMESTAMP null,
    updated_ts              timestamp default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP,
    FOREIGN KEY (partition_id) references DT_PARTITION (id)
        on delete cascade,
    FOREIGN KEY (dependency_partition_id) references DT_PARTITION (id)
        on delete cascade
);

CREATE TABLE IF NOT EXISTS DT_SOURCE
(
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    cf_table_id         BIGINT       NOT NULL,
    connection_id       BIGINT       NOT NULL,
    sql_select_query    VARCHAR(2048) DEFAULT NULL,
    sql_partition_query VARCHAR(2048) DEFAULT NULL,
    partition_column    VARCHAR(256)  DEFAULT NULL,
    query_column        VARCHAR(256)  DEFAULT NULL,
    version             VARCHAR(256) NOT NULL,
    created_ts          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_ts          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    description         VARCHAR(512),
    FOREIGN KEY (cf_table_id)
        REFERENCES CF_TABLE (id),
    FOREIGN KEY (connection_id)
        REFERENCES CF_CONNECTION (id)
);




