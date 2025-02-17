CREATE TABLE `non_pk_auto_inc`
(
    `a`   int(11) DEFAULT NULL,
    `b`   int(11) DEFAULT NULL,
    `c`   int(11) DEFAULT NULL,
    `inc` bigint(20) NOT NULL AUTO_INCREMENT,
    KEY `idx_bc` (`b`, `c`),
    KEY `idx_inc` (`inc`)
) PARTITION BY RANGE (`a`)
    (PARTITION `p0` VALUES LESS THAN (100),
    PARTITION `p1` VALUES LESS THAN (200),
    PARTITION `p2` VALUES LESS THAN (300));
