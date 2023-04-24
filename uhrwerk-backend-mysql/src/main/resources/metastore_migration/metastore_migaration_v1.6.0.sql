USE UHRWERK_METASTORE_TEST2;

CREATE TABLE IF NOT EXISTS FUNCTION_DEFINITION
(
    id                BIGINT AUTO_INCREMENT PRIMARY KEY,
    name              VARCHAR(128)                        NOT NULL,
    type              enum ('CLASS', 'SQL')               NOT NULL DEFAULT 'CLASS',
    class_name        VARCHAR(256)                        NULL,
    sql_query         TEXT                                NULL,
    params            JSON                                NULL,
    input_views       JSON                                NULL,
    output            VARCHAR(128)                        NULL,
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

CREATE TABLE IF NOT EXISTS FUNCTION_CALL
(
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id            BIGINT                              NOT NULL,
    table_key           BIGINT                              NOT NULL,
    function_key        BIGINT                              NOT NULL,
    function_name       VARCHAR(128)                        NOT NULL,
    function_call_order INT                                 NOT NULL,
    args                JSON                                NULL,
    input_views         JSON                                NULL,
    output              VARCHAR(128)                        NULL,
    deactivated_ts      TIMESTAMP                           NULL,
    deactivated_epoch   BIGINT AS (IFNULL((TIMESTAMPDIFF(second, '1970-01-01', deactivated_ts)),
                                          0)),
    created_ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    updated_ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL ON update CURRENT_TIMESTAMP,
    INDEX (table_key),
    INDEX (function_key),
    UNIQUE (table_key, function_key, function_call_order, deactivated_epoch),
    FOREIGN KEY (table_id) REFERENCES TABLE_ (id) ON DELETE CASCADE
);

