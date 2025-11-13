-- T0 时间点：初始数据
CREATE DATABASE original_db;
USE original_db;

CREATE TABLE t1 (
    id INT PRIMARY KEY,
    val VARCHAR(50)
);

INSERT INTO t1 (id, val) VALUES
    (1, 'initial_data_row1'),
    (2, 'initial_data_row2'),
    (3, 'initial_data_row3');
