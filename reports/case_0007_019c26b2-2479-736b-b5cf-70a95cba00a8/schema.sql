SET FOREIGN_KEY_CHECKS=0;
DROP VIEW IF EXISTS v4;
DROP VIEW IF EXISTS v3;
DROP VIEW IF EXISTS v2;
DROP VIEW IF EXISTS v1;
DROP VIEW IF EXISTS v0;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t0;
CREATE TABLE `t0` (
  `id` bigint NOT NULL,
  `k0` int NOT NULL,
  `k1` date NOT NULL,
  `k2` int NOT NULL,
  `k3` varchar(64) NOT NULL,
  `p0` datetime NOT NULL,
  `p1` int NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t1` (
  `id` bigint NOT NULL,
  `k0` int NOT NULL,
  `d0` float NOT NULL,
  `d1` date NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_id_1` (`id`),
  KEY `idx_d0_9` (`d0`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t2` (
  `id` bigint NOT NULL,
  `k1` date NOT NULL,
  `k0` int NOT NULL,
  `d0` date NOT NULL,
  `d1` date NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_id_2` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t3` (
  `id` bigint NOT NULL,
  `k2` int NOT NULL,
  `k0` int NOT NULL,
  `d0` int NOT NULL,
  `d1` decimal(12,2) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_k2_0` (`k2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t4` (
  `id` bigint NOT NULL,
  `k3` varchar(64) NOT NULL,
  `k0` int NOT NULL,
  `d0` tinyint(1) NOT NULL,
  `d1` varchar(64) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v0` (`g0`, `cnt`, `sum1`) AS SELECT `t4`.`id` AS `g0`,COUNT(1) AS `cnt`,SUM(`t4`.`k0`) AS `sum1` FROM (`shiro_fuzz_r6`.`t0` RIGHT JOIN `shiro_fuzz_r6`.`t2` ON (`t0`.`k0`=`t2`.`k0`)) JOIN `shiro_fuzz_r6`.`t4` ON (`t0`.`k0`=`t4`.`k0`) WHERE EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r6`.`t3` WHERE (`t3`.`k0`=`t0`.`k0`)) GROUP BY `t4`.`id` ORDER BY `t4`.`id` DESC LIMIT 1;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v1` (`cnt`, `sum1`) AS SELECT DISTINCT COUNT(1) AS `cnt`,SUM(`t3`.`d1`) AS `sum1` FROM (((`shiro_fuzz_r6`.`t0` JOIN `shiro_fuzz_r6`.`t3` ON (`t0`.`k0`=`t3`.`k0`)) RIGHT JOIN `shiro_fuzz_r6`.`t2` ON (`t0`.`k0`=`t2`.`k0`)) LEFT JOIN `shiro_fuzz_r6`.`t1` ON (`t0`.`k0`=`t1`.`k0`)) RIGHT JOIN `shiro_fuzz_r6`.`t4` ON (`t0`.`k0`=`t4`.`k0`) WHERE NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r6`.`t0` WHERE ((`t0`.`k2`=`t0`.`k2`) AND (`t0`.`k0`<=>`t0`.`k0`)));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v2` (`c0`, `c1`) AS SELECT (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r6`.`t1` WHERE ((`t1`.`k0`=`t0`.`k0`) AND (`t1`.`k0`<`t0`.`k0`))) AS `c0`,(`t0`.`id`-`t3`.`k0`) AS `c1` FROM (`shiro_fuzz_r6`.`t0` JOIN `shiro_fuzz_r6`.`t3` ON (`t0`.`k0`=`t3`.`k0`)) RIGHT JOIN `shiro_fuzz_r6`.`t2` ON (`t0`.`k0`=`t2`.`k0`) WHERE (EXISTS (SELECT `t1`.`d1` AS `c0` FROM `shiro_fuzz_r6`.`t1` WHERE ((`t1`.`k0`=`t0`.`k0`) AND (`t1`.`k0`<`t0`.`k0`)) ORDER BY `t1`.`d0` LIMIT 1) AND NOT (`t0`.`k0` IN (52)));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v3` (`c0`, `c1`) AS SELECT DENSE_RANK() OVER (ORDER BY `t3`.`id` DESC,`t0`.`k3`) AS `c0`,56 AS `c1` FROM (((`shiro_fuzz_r6`.`t0` RIGHT JOIN `shiro_fuzz_r6`.`t4` ON (`t0`.`k0`=`t4`.`k0`)) RIGHT JOIN `shiro_fuzz_r6`.`t3` ON (`t0`.`k0`=`t3`.`k0`)) LEFT JOIN `shiro_fuzz_r6`.`t2` ON (`t0`.`k0`=`t2`.`k0`)) RIGHT JOIN `shiro_fuzz_r6`.`t1` ON (`t0`.`k0`=`t1`.`k0`) WHERE (`t0`.`k0` IN (28,9,7));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v4` (`c0`, `c1`, `c2`) AS SELECT 21 AS `c0`,AVG(`t3`.`d1`) OVER (ORDER BY `t2`.`d0`) AS `c1`,_UTF8MB4'2025-05-28 09:20:28' AS `c2` FROM (((`shiro_fuzz_r6`.`t0` RIGHT JOIN `shiro_fuzz_r6`.`t3` ON (`t0`.`k0`=`t3`.`k0`)) JOIN `shiro_fuzz_r6`.`t2` ON (`t0`.`k0`=`t2`.`k0`)) RIGHT JOIN `shiro_fuzz_r6`.`t4` ON (`t0`.`k0`=`t4`.`k0`)) LEFT JOIN `shiro_fuzz_r6`.`t1` ON (`t0`.`k0`=`t1`.`k0`) WHERE ((`t2`.`k0`>=`t1`.`k0`) AND (`t4`.`k0`<`t1`.`k0`));

SET FOREIGN_KEY_CHECKS=1;
