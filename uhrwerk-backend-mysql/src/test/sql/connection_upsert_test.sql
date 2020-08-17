INSERT INTO CONNECTION(name, type, path)
VALUES ('fs_connection_1', 'JDBC', '/some/test/path');

SELECT id,name,type,path,created_ts,updated_ts
FROM CONNECTION
WHERE name = 'fs_connection_1';

INSERT INTO CONNECTION(name, type, path)
VALUES ('fs_connection_1', 'FS', '/new/path/this_is_NEW')
ON DUPLICATE KEY UPDATE type='FS';

INSERT INTO CONNECTION(name, type, path)
VALUES ('fs_connection_1', 'FS', '/new/path/this_is_NEW')
ON DUPLICATE KEY UPDATE type='FS',
                        path = '/new/path/this_is_NEW';
DELETE
FROM CONNECTION
WHERE name = 'fs_connection_1';
