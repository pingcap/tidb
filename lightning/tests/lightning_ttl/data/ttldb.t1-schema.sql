CREATE TABLE `t1` (
    `id` int(11) PRIMARY KEY,
    `t` datetime
) TTL = `t` + INTERVAL 1 DAY TTL_ENABLE = 'ON';
