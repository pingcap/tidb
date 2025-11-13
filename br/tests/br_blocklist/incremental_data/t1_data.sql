-- T1 时间点：创建 t2 表并插入数据
USE original_db;

-- 向 t1 插入更多数据
INSERT INTO t1 (id, val) VALUES (4, 'data_at_t1');

-- 创建新表 t2
CREATE TABLE t2 (
    id INT PRIMARY KEY,
    description TEXT
);

INSERT INTO t2 (id, description) VALUES
    (100, 't2_data_row1'),
    (101, 't2_data_row2');
