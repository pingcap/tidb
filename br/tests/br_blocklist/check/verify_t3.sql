-- 验证 T3 时间点的数据
USE original_db;

-- 应该有 4 行数据
SELECT COUNT(*) AS t1_count FROM t1;

-- 验证 id=4 的记录被更新
SELECT val FROM t1 WHERE id = 4;

-- new_db 应该存在
USE new_db;

-- 应该有 5 行数据 (2 from T2 + 3 from T3)
SELECT COUNT(*) AS t3_count FROM t3;
