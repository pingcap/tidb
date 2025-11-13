-- 验证 T2 时间点的数据
USE original_db;

-- 应该有 4 行数据
SELECT COUNT(*) AS t1_count FROM t1;

-- t2 表应该存在
SELECT COUNT(*) AS t2_count FROM t2;

-- new_db 应该存在
USE new_db;
SELECT COUNT(*) AS t3_count FROM t3;
