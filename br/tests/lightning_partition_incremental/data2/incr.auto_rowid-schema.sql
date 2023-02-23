CREATE TABLE `auto_rowid`
(
    `a` int(11) DEFAULT NULL,
    `b` int(11) DEFAULT NULL,
    `c` int(11) DEFAULT NULL,
    KEY `idx_bc` (`b`, `c`)
) PARTITION BY RANGE (`a`)
    (PARTITION `p0` VALUES LESS THAN (100),
    PARTITION `p1` VALUES LESS THAN (200),
    PARTITION `p2` VALUES LESS THAN (300));
