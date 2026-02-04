SET FOREIGN_KEY_CHECKS=0;
DROP VIEW IF EXISTS v4;
DROP VIEW IF EXISTS v3;
DROP VIEW IF EXISTS v2;
DROP VIEW IF EXISTS v1;
DROP VIEW IF EXISTS v0;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t0;
CREATE TABLE `t0` (
  `id` bigint NOT NULL,
  `c0` bigint DEFAULT NULL,
  `c1` decimal(12,2) DEFAULT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c1` (`c1`),
  KEY `fk_0` (`id`),
  KEY `idx_id_0` (`id`),
  KEY `idx_c0_2` (`c0`),
  CONSTRAINT `fk_0` FOREIGN KEY (`id`) REFERENCES `t1` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t1` (
  `id` bigint NOT NULL,
  `c0` timestamp NOT NULL,
  `c1` int NOT NULL,
  `c2` timestamp NOT NULL,
  `c3` bigint NOT NULL,
  `c4` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t2` (
  `id` bigint NOT NULL,
  `c0` varchar(64) NOT NULL,
  `c1` float NOT NULL,
  `c2` date NOT NULL,
  `c3` double DEFAULT NULL,
  `c4` float NOT NULL,
  `c5` tinyint(1) NOT NULL,
  `c6` double DEFAULT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c0` (`c0`),
  KEY `idx_c2` (`c2`),
  KEY `idx_c6_1` (`c6`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY HASH (`id`) PARTITIONS 2;

CREATE TABLE `t3` (
  `id` bigint NOT NULL,
  `c0` tinyint(1) NOT NULL,
  `c1` varchar(64) NOT NULL,
  `c2` decimal(12,2) NOT NULL,
  `c3` date NOT NULL,
  `c4` timestamp NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v0` (`cnt`, `sum1`) AS SELECT DISTINCT COUNT(1) AS `cnt`,SUM(`t0`.`c1`) AS `sum1` FROM `shiro_fuzz_r11`.`t0` RIGHT JOIN `shiro_fuzz_r11`.`t1` ON (1=0) WHERE NOT (`t0`.`c0` IN (24));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v1` (`c0`, `c1`, `c2`) AS SELECT (`v0`.`sum1`*`t0`.`id`) AS `c0`,(SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r11`.`t1` WHERE (`t1`.`c1`=`v0`.`cnt`)) AS `c1`,(`t1`.`c1`-`v0`.`cnt`) AS `c2` FROM (`shiro_fuzz_r11`.`t0` LEFT JOIN `shiro_fuzz_r11`.`v0` ON (1=0)) JOIN `shiro_fuzz_r11`.`t1` ON (1=0) WHERE NOT EXISTS (SELECT `t1`.`c3` AS `c0` FROM `shiro_fuzz_r11`.`t1` WHERE EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r11`.`t0` WHERE ((`t0`.`c0`>`t0`.`c1`) AND (`t0`.`id`<=`t0`.`c1`)) LIMIT 1));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v2` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t0`.`c1`) AS `sum1` FROM ((`shiro_fuzz_r11`.`v1` JOIN `shiro_fuzz_r11`.`v0` ON (1=0)) LEFT JOIN `shiro_fuzz_r11`.`t0` ON (1=0)) LEFT JOIN `shiro_fuzz_r11`.`t1` ON (1=0) WHERE (NOT EXISTS (SELECT `t0`.`c0` AS `c0` FROM `shiro_fuzz_r11`.`t0` WHERE ((`t0`.`id`!=`t0`.`c1`) AND (`t0`.`id`<=>`t0`.`c1`))) OR EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r11`.`t1` WHERE NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r11`.`t0` WHERE ((`t0`.`id`<`t0`.`c1`) OR (`t0`.`id`<=`t0`.`c1`))) LIMIT 4));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v3` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t1`.`c3`) AS `sum1` FROM ((`shiro_fuzz_r11`.`v1` RIGHT JOIN `shiro_fuzz_r11`.`v2` ON (1=0)) JOIN `shiro_fuzz_r11`.`t0` ON (1=0)) JOIN `shiro_fuzz_r11`.`t1` ON (1=0) WHERE NOT EXISTS (SELECT `t1`.`c0` AS `c0` FROM `shiro_fuzz_r11`.`t1` WHERE (`t1`.`c0`>`t1`.`c2`)) ORDER BY COUNT(1);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v4` (`c0`, `c1`, `c2`) AS SELECT DISTINCT `t1`.`id` AS `c0`,(`v2`.`sum1`+`v2`.`sum1`) AS `c1`,(`v0`.`cnt`-`v2`.`cnt`) AS `c2` FROM ((((`shiro_fuzz_r11`.`t1` RIGHT JOIN `shiro_fuzz_r11`.`t0` ON (1=0)) LEFT JOIN `shiro_fuzz_r11`.`v1` ON (1=0)) RIGHT JOIN `shiro_fuzz_r11`.`v2` ON (1=0)) RIGHT JOIN `shiro_fuzz_r11`.`v3` ON (1=0)) LEFT JOIN `shiro_fuzz_r11`.`v0` ON (1=0) WHERE (NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r11`.`v2` WHERE ((`v2`.`cnt`=`v2`.`sum1`) OR (`v2`.`cnt`<=>`v2`.`sum1`)) LIMIT 4) OR ((`t1`.`c0`=`t1`.`c2`) AND (`t1`.`id`!=`v2`.`sum1`))) ORDER BY (`v2`.`sum1`+`v2`.`sum1`) DESC,`t1`.`id` DESC LIMIT 2;

SET FOREIGN_KEY_CHECKS=1;
