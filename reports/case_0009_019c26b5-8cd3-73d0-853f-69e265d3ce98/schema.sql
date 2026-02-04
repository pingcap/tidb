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
  `k1` int NOT NULL,
  `k2` bigint NOT NULL,
  `k3` int NOT NULL,
  `p0` decimal(12,2) NOT NULL,
  `p1` datetime NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_p1_1` (`p1`),
  KEY `idx_k3_7` (`k3`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t1` (
  `id` bigint NOT NULL,
  `k0` int NOT NULL,
  `d0` decimal(12,2) NOT NULL,
  `d1` double NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_k0_3` (`k0`),
  KEY `idx_id_4` (`id`),
  KEY `idx_d1_5` (`d1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t2` (
  `id` bigint NOT NULL,
  `k1` int NOT NULL,
  `k0` int NOT NULL,
  `d0` tinyint(1) NOT NULL,
  `d1` int NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_d0_11` (`d0`),
  KEY `idx_k0_12` (`k0`),
  KEY `idx_d1_13` (`d1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t3` (
  `id` bigint NOT NULL,
  `k2` bigint NOT NULL,
  `k0` int NOT NULL,
  `d0` date NOT NULL,
  `d1` double NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t4` (
  `id` bigint NOT NULL,
  `k3` int NOT NULL,
  `k0` int NOT NULL,
  `d0` float NOT NULL,
  `d1` int NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v0` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t0`.`p0`) AS `sum1` FROM (`shiro_fuzz_r8`.`t0` LEFT JOIN `shiro_fuzz_r8`.`t2` ON (`t0`.`k0`=`t2`.`k0`)) LEFT JOIN `shiro_fuzz_r8`.`t3` ON (`t0`.`k0`=`t3`.`k0`) WHERE (`t0`.`k0`=`t3`.`k0`);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v1` (`c0`, `c1`) AS SELECT (`t1`.`k0`-`t3`.`d1`) AS `c0`,ROUND(`t2`.`k1`) AS `c1` FROM ((`shiro_fuzz_r8`.`t0` RIGHT JOIN `shiro_fuzz_r8`.`t1` ON (`t0`.`k0`=`t1`.`k0`)) RIGHT JOIN `shiro_fuzz_r8`.`t2` ON (`t0`.`k0`=`t2`.`k0`)) RIGHT JOIN `shiro_fuzz_r8`.`t3` ON (`t0`.`k0`=`t3`.`k0`) WHERE EXISTS (SELECT `t0`.`p1` AS `c0` FROM `shiro_fuzz_r8`.`t0` WHERE (`t0`.`k0`=`t0`.`k0`));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v2` (`c0`, `c1`, `c2`) AS SELECT DENSE_RANK() OVER (ORDER BY `t3`.`k0` DESC,`t3`.`d0` DESC) AS `c0`,(`t4`.`id`-`t1`.`d0`) AS `c1`,29.31 AS `c2` FROM ((`shiro_fuzz_r8`.`t0` JOIN `shiro_fuzz_r8`.`t1` ON (`t0`.`k0`=`t1`.`k0`)) RIGHT JOIN `shiro_fuzz_r8`.`t4` ON (`t0`.`k0`=`t4`.`k0`)) LEFT JOIN `shiro_fuzz_r8`.`t3` ON (`t0`.`k0`=`t3`.`k0`) WHERE NOT EXISTS (SELECT `v0`.`sum1` AS `c0` FROM `shiro_fuzz_r8`.`v0` WHERE ((`v0`.`sum1`<`v0`.`cnt`) AND (`v0`.`cnt`<=`v0`.`sum1`)) ORDER BY ABS(`v0`.`sum1`) LIMIT 3) ORDER BY `t1`.`d1`,`t3`.`d0` DESC;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v3` (`g0`, `cnt`, `sum1`) AS SELECT DISTINCT `t2`.`id` AS `g0`,COUNT(1) AS `cnt`,SUM(`t1`.`d0`) AS `sum1` FROM ((`shiro_fuzz_r8`.`t0` LEFT JOIN `shiro_fuzz_r8`.`t4` ON (`t0`.`k0`=`t4`.`k0`)) LEFT JOIN `shiro_fuzz_r8`.`t2` ON (`t0`.`k0`=`t2`.`k0`)) LEFT JOIN `shiro_fuzz_r8`.`t1` ON (`t0`.`k0`=`t1`.`k0`) WHERE (NOT EXISTS (SELECT `t0`.`p1` AS `c0` FROM `shiro_fuzz_r8`.`t0` WHERE (`t0`.`k2`=`t0`.`k2`)) AND NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r8`.`v0` WHERE ((`v0`.`sum1`!=`v0`.`cnt`) AND (`v0`.`cnt`<=>`v0`.`sum1`)) ORDER BY COUNT(1) DESC LIMIT 4)) GROUP BY `t2`.`id` ORDER BY SUM(`shiro_fuzz_r8`.`t1`.`d0`) DESC;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v4` (`c0`) AS SELECT ROUND(`t1`.`k0`) AS `c0` FROM (`shiro_fuzz_r8`.`t0` JOIN `shiro_fuzz_r8`.`t1` ON (`t0`.`k0`=`t1`.`k0`)) LEFT JOIN `shiro_fuzz_r8`.`t4` ON (`t0`.`k0`=`t4`.`k0`) WHERE (NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r8`.`v1` WHERE ((`v1`.`c1`<=60) AND (`v1`.`c1`>8))) OR (NOT EXISTS (SELECT `v2`.`c2` AS `c0` FROM `shiro_fuzz_r8`.`v2` WHERE ((`v2`.`c0`>`v2`.`c1`) AND (`v2`.`c1`<=>`v2`.`c0`)) LIMIT 5) AND NOT (`t0`.`k0` IN (27,73))));

SET FOREIGN_KEY_CHECKS=1;
