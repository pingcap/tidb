SET FOREIGN_KEY_CHECKS=0;
DROP VIEW IF EXISTS v5;
DROP VIEW IF EXISTS v4;
DROP VIEW IF EXISTS v3;
DROP VIEW IF EXISTS v2;
DROP VIEW IF EXISTS v1;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t0;
CREATE TABLE `t0` (
  `id` bigint NOT NULL,
  `k0` varchar(64) NOT NULL,
  `k1` varchar(64) NOT NULL,
  `k2` int NOT NULL,
  `k3` int NOT NULL,
  `p0` float NOT NULL,
  `p1` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t1` (
  `id` bigint NOT NULL,
  `k0` varchar(64) NOT NULL,
  `d0` bigint NOT NULL,
  `d1` double NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t2` (
  `id` bigint NOT NULL,
  `k1` varchar(64) NOT NULL,
  `k0` varchar(64) NOT NULL,
  `d0` decimal(12,2) NOT NULL,
  `d1` double NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t3` (
  `id` bigint NOT NULL,
  `k2` int NOT NULL,
  `k0` varchar(64) NOT NULL,
  `d0` float NOT NULL,
  `d1` date NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_id_4` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t4` (
  `id` bigint NOT NULL,
  `k3` int NOT NULL,
  `k0` varchar(64) NOT NULL,
  `d0` date NOT NULL,
  `d1` bigint NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v1` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t2`.`d0`) AS `sum1` FROM ((`shiro_fuzz_r2`.`t0` LEFT JOIN `shiro_fuzz_r2`.`t2` ON ((`t0`.`k0`=`t2`.`k0`) AND ((`t0`.`k0`>=`t2`.`k0`) AND (`t0`.`k0`>=`t2`.`k0`)))) JOIN `shiro_fuzz_r2`.`t3` ON ((`t0`.`k0`=`t3`.`k0`) AND ((`t2`.`k0`!=`t3`.`k0`) AND (`t0`.`k0`<`t3`.`k0`)))) LEFT JOIN `shiro_fuzz_r2`.`t4` ON ((`t0`.`k0`=`t4`.`k0`) AND (`t0`.`k0`<=`t3`.`k0`)) WHERE NOT EXISTS (SELECT `t2`.`id` AS `c0` FROM `shiro_fuzz_r2`.`t2` WHERE (`t2`.`k0`=`t0`.`k0`));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v2` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t0`.`k2`) AS `sum1` FROM (`shiro_fuzz_r2`.`t0` JOIN `shiro_fuzz_r2`.`t1` ON ((`t0`.`k0`=`t1`.`k0`) AND ((`t0`.`k0`!=`t1`.`k0`) AND (`t0`.`k0`<=>`t1`.`k0`)))) RIGHT JOIN `shiro_fuzz_r2`.`t3` ON ((`t0`.`k0`=`t3`.`k0`) AND ((`t1`.`k0`<=>`t3`.`k0`) AND (`t0`.`k0`>=`t3`.`k0`))) WHERE (`t0`.`k0` IN (_UTF8MB4's17',_UTF8MB4's16',_UTF8MB4's95'));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v3` (`c0`, `c1`) AS SELECT (`t2`.`d1`*`t1`.`id`) AS `c0`,_UTF8MB4's83' AS `c1` FROM (((`shiro_fuzz_r2`.`t0` LEFT JOIN `shiro_fuzz_r2`.`t2` ON ((`t0`.`k0`=`t2`.`k0`) AND (`t0`.`k0`<`t2`.`k0`))) JOIN `shiro_fuzz_r2`.`t4` ON ((`t0`.`k0`=`t4`.`k0`) AND (`t2`.`k0` IN (_UTF8MB4's19',_UTF8MB4's88',_UTF8MB4's69')))) LEFT JOIN `shiro_fuzz_r2`.`t3` ON ((`t0`.`k0`=`t3`.`k0`) AND NOT (`t4`.`k0` IN (_UTF8MB4's45',_UTF8MB4's91')))) RIGHT JOIN `shiro_fuzz_r2`.`t1` ON ((`t0`.`k0`=`t1`.`k0`) AND ((`t0`.`k0`<=`t1`.`k0`) AND (`t2`.`k0`>`t3`.`k0`))) WHERE NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r2`.`v2` WHERE ((`v2`.`sum1`>=`v2`.`cnt`) OR (`v2`.`cnt`!=`v2`.`sum1`)) LIMIT 5) ORDER BY (`t2`.`id`+`t4`.`d1`) LIMIT 15;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v4` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t2`.`d0`) AS `sum1` FROM (`shiro_fuzz_r2`.`t0` RIGHT JOIN `shiro_fuzz_r2`.`t4` USING (`k0`)) RIGHT JOIN `shiro_fuzz_r2`.`t2` ON ((`t0`.`k0`=`t2`.`k0`) AND NOT (`t0`.`k0` IN (_UTF8MB4's23'))) WHERE EXISTS (SELECT `t0`.`k1` AS `c0` FROM `shiro_fuzz_r2`.`t0` WHERE (`t0`.`k3`=`t0`.`k3`));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v5` (`c0`) AS SELECT ROUND(`t2`.`d0`) AS `c0` FROM (`shiro_fuzz_r2`.`t0` RIGHT JOIN `shiro_fuzz_r2`.`t2` ON ((`t0`.`k0`=`t2`.`k0`) AND (`t0`.`k0`<=>`t2`.`k0`))) JOIN `shiro_fuzz_r2`.`t1` ON ((`t0`.`k0`=`t1`.`k0`) AND (`t0`.`k0`<`t2`.`k0`)) WHERE (`t0`.`k0`>=`t1`.`k0`) ORDER BY `t0`.`p1`,`t0`.`k2` DESC;

SET FOREIGN_KEY_CHECKS=1;
