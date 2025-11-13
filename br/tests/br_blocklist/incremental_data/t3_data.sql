-- T3 时间点：对 new_db 进行更多操作
USE new_db;
INSERT INTO t3 (id, description) VALUES
    (202, 'data_at_t3_row1'),
    (203, 'data_at_t3_row2'),
    (204, 'data_at_t3_row3');

USE original_db;
UPDATE t1 SET val = 'updated_at_t3' WHERE id = 4;
