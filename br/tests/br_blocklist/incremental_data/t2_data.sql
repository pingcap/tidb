-- T2 时间点：创建 new_db 和 t3 表
CREATE DATABASE new_db;
USE new_db;

CREATE TABLE t3 (
    id INT PRIMARY KEY,
    description TEXT
);

INSERT INTO t3 (id, description) VALUES
    (200, 'new_db_data'),
    (201, 'created_at_t2');
