CREATE TABLE `orders` (
    `region` varchar(32) NOT NULL,
    `id`     int         NOT NULL,
    `amount` int         NOT NULL
) PARTITION BY LIST COLUMNS (`region`) (
    PARTITION `p_a` VALUES IN ('alpha'),
    PARTITION `p_b` VALUES IN ('beta')
);
