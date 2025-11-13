-- 验证 T1 时间点的数据
USE original_db;

-- 应该有 4 行数据
SELECT COUNT(*) AS t1_count FROM t1;

-- 应该有 2 行数据
SELECT COUNT(*) AS t2_count FROM t2;
