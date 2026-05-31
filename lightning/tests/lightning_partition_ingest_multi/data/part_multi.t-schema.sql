CREATE TABLE `t` (
    `region` varchar(32) NOT NULL,
    `val`    int         NOT NULL
) PARTITION BY LIST COLUMNS (`region`) (
    PARTITION `p_a` VALUES IN ('alpha'),
    PARTITION `p_b` VALUES IN ('beta'),
    PARTITION `p_c` VALUES IN ('gamma')
);
