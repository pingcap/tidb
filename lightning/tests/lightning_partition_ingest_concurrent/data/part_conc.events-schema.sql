CREATE TABLE `events` (
    `region` varchar(32) NOT NULL,
    `ts`     bigint      NOT NULL,
    `msg`    varchar(64) NOT NULL
) PARTITION BY LIST COLUMNS (`region`) (
    PARTITION `p_a` VALUES IN ('alpha'),
    PARTITION `p_b` VALUES IN ('beta')
);
