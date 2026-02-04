SET FOREIGN_KEY_CHECKS=0;
DROP VIEW IF EXISTS v4;
DROP VIEW IF EXISTS v3;
DROP VIEW IF EXISTS v2;
DROP VIEW IF EXISTS v1;
DROP VIEW IF EXISTS v0;
DROP TABLE IF EXISTS t5;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t0;
CREATE TABLE `t0` (
  `id` bigint NOT NULL,
  `c0` datetime NOT NULL,
  `c1` datetime NOT NULL,
  `c2` datetime NOT NULL,
  `c3` date DEFAULT NULL,
  `c4` datetime DEFAULT NULL,
  `c5` decimal(12,2) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c1` (`c1`),
  KEY `idx_c2_1` (`c2`),
  KEY `idx_c3_8` (`c3`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t1` (
  `id` bigint NOT NULL,
  `c0` decimal(12,2) NOT NULL,
  `c1` date NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c0` (`c0`),
  KEY `idx_c1_0` (`c1`),
  KEY `fk_1` (`id`),
  KEY `idx_c0_c1_4` (`c0`,`c1`),
  KEY `idx_id_11` (`id`),
  CONSTRAINT `fk_1` FOREIGN KEY (`id`) REFERENCES `t0` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t2` (
  `id` bigint NOT NULL,
  `c0` tinyint(1) NOT NULL,
  `c1` date NOT NULL,
  `c2` int NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_id_2` (`id`),
  KEY `idx_c2_3` (`c2`),
  KEY `idx_c1_4` (`c1`),
  KEY `idx_c0_7` (`c0`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t3` (
  `id` bigint NOT NULL,
  `c0` bigint NOT NULL,
  `c1` date DEFAULT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_id_5` (`id`),
  KEY `idx_c1_13` (`c1`),
  KEY `idx_c0_14` (`c0`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY HASH (`id`) PARTITIONS 3;

CREATE TABLE `t4` (
  `id` bigint NOT NULL,
  `c0` decimal(12,2) NOT NULL,
  `c1` datetime DEFAULT NULL,
  `c2` double NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c0` (`c0`),
  KEY `idx_c1` (`c1`),
  KEY `idx_id_9` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY HASH (`id`) PARTITIONS 3;

CREATE TABLE `t5` (
  `id` bigint NOT NULL,
  `c0` datetime NOT NULL,
  `c1` timestamp NOT NULL,
  `c2` date NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c1` (`c1`),
  KEY `idx_c2` (`c2`),
  KEY `idx_id_6` (`id`),
  KEY `idx_c0_10` (`c0`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v0` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t0`.`c5`) AS `sum1` FROM `shiro_fuzz_r3`.`t0` LEFT JOIN `shiro_fuzz_r3`.`t1` ON (1=0) WHERE ((`t0`.`c3` IN (_UTF8MB4'2025-08-22',_UTF8MB4'2025-06-28')) AND ((`t1`.`id`<`t0`.`c5`) OR (`t1`.`c0`=`t0`.`id`)));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v1` (`c0`, `c1`) AS SELECT UPPER(_UTF8MB4's10') AS `c0`,UPPER(_UTF8MB4's39') AS `c1` FROM (`shiro_fuzz_r3`.`v0` LEFT JOIN `shiro_fuzz_r3`.`t0` ON (1=0)) JOIN `shiro_fuzz_r3`.`t1` ON (1=0) WHERE NOT EXISTS (SELECT `t0`.`id` AS `c0` FROM `shiro_fuzz_r3`.`t0` WHERE ((`t0`.`c5`<=`t0`.`id`) AND (`t0`.`id`<=`t0`.`c5`)));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v2` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`v0`.`cnt`) AS `sum1` FROM ((`shiro_fuzz_r3`.`t0` JOIN `shiro_fuzz_r3`.`v1` ON (1=0)) JOIN `shiro_fuzz_r3`.`v0` ON (1=0)) LEFT JOIN `shiro_fuzz_r3`.`t1` ON (1=0) WHERE (`t0`.`c1`<=>`t0`.`c4`);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v3` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t0`.`c5`) AS `sum1` FROM (`shiro_fuzz_r3`.`v0` JOIN `shiro_fuzz_r3`.`t1` ON (1=0)) LEFT JOIN `shiro_fuzz_r3`.`t0` ON (1=0) WHERE EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r3`.`v2` WHERE (NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r3`.`t0` WHERE (`t0`.`c3`>=`t0`.`c0`)) AND NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r3`.`t0` WHERE ((`t0`.`c2`<=>`t0`.`c1`) AND (`t0`.`c1`<`t0`.`c2`)) LIMIT 5))) ORDER BY 1,2 DESC;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v4` (`g0`, `g1`, `cnt`, `sum1`) AS SELECT `t0`.`id` AS `g0`,`t0`.`c4` AS `g1`,COUNT(1) AS `cnt`,SUM(`v2`.`cnt`) AS `sum1` FROM ((`shiro_fuzz_r3`.`v0` JOIN `shiro_fuzz_r3`.`t1` ON (1=0)) JOIN `shiro_fuzz_r3`.`v2` ON (1=0)) LEFT JOIN `shiro_fuzz_r3`.`t0` ON (1=0) WHERE NOT (`v2`.`cnt` IN (38,56,16)) GROUP BY `t0`.`id`,`t0`.`c4` ORDER BY 1,2;

SET FOREIGN_KEY_CHECKS=1;
