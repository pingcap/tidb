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
  KEY `idx_c4_0` (`c4`),
  KEY `fk_3` (`id`),
  KEY `idx_id_2` (`id`),
  KEY `idx_c3_8` (`c3`),
  KEY `idx_c5_9` (`c5`),
  KEY `idx_c0_13` (`c0`),
  KEY `idx_c2_14` (`c2`),
  KEY `idx_c0_c2_c5_29` (`c0`,`c2`,`c5`),
  KEY `idx_c4_c1_c2_29` (`c4`,`c1`,`c2`),
  KEY `idx_c5_c4_30` (`c5`,`c4`),
  KEY `idx_c1_c4_33` (`c1`,`c4`),
  KEY `idx_c4_c3_c2_36` (`c4`,`c3`,`c2`),
  KEY `idx_c2_c5_c0_36` (`c2`,`c5`,`c0`),
  KEY `idx_c5_c0_37` (`c5`,`c0`),
  CONSTRAINT `fk_3` FOREIGN KEY (`id`) REFERENCES `t1` (`id`),
  CONSTRAINT `fk_113` FOREIGN KEY (`id`) REFERENCES `t2` (`id`),
  CONSTRAINT `fk_195` FOREIGN KEY (`id`) REFERENCES `t4` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t1` (
  `id` bigint NOT NULL,
  `c0` decimal(12,2) NOT NULL,
  `c1` date NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c0` (`c0`),
  KEY `idx_c1_1` (`c1`),
  KEY `idx_id_5` (`id`),
  KEY `idx_c0_c1_30` (`c0`,`c1`),
  KEY `idx_c1_c0_30` (`c1`,`c0`),
  CONSTRAINT `fk_130` FOREIGN KEY (`id`) REFERENCES `t4` (`id`),
  CONSTRAINT `fk_214` FOREIGN KEY (`id`) REFERENCES `t2` (`id`),
  CONSTRAINT `fk_230` FOREIGN KEY (`id`) REFERENCES `t0` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t2` (
  `id` bigint NOT NULL,
  `c0` date NOT NULL,
  `c1` varchar(64) NOT NULL,
  `c2` double NOT NULL,
  `c3` float NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c0_c1_c2_3` (`c0`,`c1`,`c2`),
  KEY `idx_c3_c0_4` (`c3`,`c0`),
  KEY `idx_c1_11` (`c1`),
  KEY `idx_id_16` (`id`),
  KEY `idx_c3_17` (`c3`),
  KEY `idx_c0_20` (`c0`),
  KEY `idx_c2_21` (`c2`),
  KEY `idx_c2_c0_c3_29` (`c2`,`c0`,`c3`),
  KEY `idx_c0_c3_c2_31` (`c0`,`c3`,`c2`),
  KEY `idx_c1_c0_33` (`c1`,`c0`),
  KEY `idx_c0_c3_36` (`c0`,`c3`),
  KEY `idx_c0_c2_37` (`c0`,`c2`),
  KEY `idx_c1_c3_37` (`c1`,`c3`),
  KEY `idx_c0_c1_37` (`c0`,`c1`),
  CONSTRAINT `fk_88` FOREIGN KEY (`id`) REFERENCES `t4` (`id`),
  CONSTRAINT `fk_123` FOREIGN KEY (`id`) REFERENCES `t0` (`id`),
  CONSTRAINT `fk_179` FOREIGN KEY (`id`) REFERENCES `t1` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t3` (
  `id` bigint NOT NULL,
  `c0` tinyint(1) NOT NULL,
  `c1` double NOT NULL,
  `c2` tinyint(1) NOT NULL,
  `c3` double NOT NULL,
  `c4` timestamp NOT NULL,
  `c5` datetime NOT NULL,
  `c6` float NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c2` (`c2`),
  KEY `idx_c4` (`c4`),
  KEY `idx_c5_c0_6` (`c5`,`c0`),
  KEY `idx_c5_c0_c1_7` (`c5`,`c0`,`c1`),
  KEY `idx_c3_12` (`c3`),
  KEY `idx_c0_15` (`c0`),
  KEY `idx_c1_19` (`c1`),
  KEY `idx_c5_22` (`c5`),
  KEY `idx_c0_c5_23` (`c0`,`c5`),
  KEY `idx_c6_24` (`c6`),
  KEY `idx_id_26` (`id`),
  KEY `idx_c1_c2_c3_29` (`c1`,`c2`,`c3`),
  KEY `idx_c6_c5_31` (`c6`,`c5`),
  KEY `idx_c5_c2_c1_34` (`c5`,`c2`,`c1`),
  KEY `idx_c1_c3_c4_34` (`c1`,`c3`,`c4`),
  KEY `idx_c1_c5_37` (`c1`,`c5`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY HASH (`id`) PARTITIONS 2;

CREATE TABLE `t4` (
  `id` bigint NOT NULL,
  `c0` tinyint(1) NOT NULL,
  `c1` datetime NOT NULL,
  `c2` double NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c1` (`c1`),
  KEY `fk_50` (`id`),
  KEY `idx_id_23` (`id`),
  KEY `idx_c0_25` (`c0`),
  KEY `idx_c2_27` (`c2`),
  KEY `idx_c0_c2_34` (`c0`,`c2`),
  KEY `idx_c1_c0_37` (`c1`,`c0`),
  CONSTRAINT `fk_50` FOREIGN KEY (`id`) REFERENCES `t0` (`id`),
  CONSTRAINT `fk_78` FOREIGN KEY (`id`) REFERENCES `t2` (`id`),
  CONSTRAINT `fk_182` FOREIGN KEY (`id`) REFERENCES `t1` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t5` (
  `id` bigint NOT NULL,
  `c0` varchar(64) NOT NULL,
  `c1` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c1` (`c1`),
  KEY `idx_c0_10` (`c0`),
  KEY `idx_id_18` (`id`),
  KEY `idx_c0_c1_29` (`c0`,`c1`),
  KEY `idx_c1_c0_29` (`c1`,`c0`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY HASH (`id`) PARTITIONS 2;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v0` (`g0`, `cnt`, `sum1`) AS SELECT `t1`.`id` AS `g0`,COUNT(1) AS `cnt`,SUM(`t1`.`c0`) AS `sum1` FROM `shiro_fuzz_r3`.`t0` RIGHT JOIN `shiro_fuzz_r3`.`t1` ON (1=0) WHERE EXISTS (SELECT `t1`.`c1` AS `c0` FROM `shiro_fuzz_r3`.`t1` WHERE ((`t1`.`c0`<=>`t1`.`id`) AND (`t1`.`id`<=`t1`.`c0`))) GROUP BY `t1`.`id`;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v1` (`c0`, `c1`, `c2`) AS SELECT RANK() OVER (ORDER BY `t1`.`id`,`v0`.`sum1` DESC) AS `c0`,(SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r3`.`v0` WHERE (`v0`.`cnt`=`v0`.`sum1`)) AS `c1`,RANK() OVER (ORDER BY `t0`.`c2`) AS `c2` FROM (`shiro_fuzz_r3`.`t1` RIGHT JOIN `shiro_fuzz_r3`.`v0` ON (1=0)) JOIN `shiro_fuzz_r3`.`t0` ON (1=0) WHERE (`t0`.`c5`<=>`t1`.`c0`);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v2` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`v0`.`cnt`) AS `sum1` FROM (`shiro_fuzz_r3`.`t0` JOIN `shiro_fuzz_r3`.`t1` ON (1=0)) LEFT JOIN `shiro_fuzz_r3`.`v0` ON (1=0) WHERE NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r3`.`v0` WHERE EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r3`.`t0` WHERE ((`t0`.`id`<=>`t0`.`c5`) OR (`t0`.`c4`<=`t0`.`c1`)))) ORDER BY SUM(`shiro_fuzz_r3`.`v0`.`cnt`);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v3` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`v2`.`sum1`) AS `sum1` FROM ((`shiro_fuzz_r3`.`t0` LEFT JOIN `shiro_fuzz_r3`.`v1` ON (1=0)) RIGHT JOIN `shiro_fuzz_r3`.`v2` ON (1=0)) LEFT JOIN `shiro_fuzz_r3`.`t1` ON (1=0) WHERE NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r3`.`v0` WHERE ((`v0`.`cnt`<=>`v0`.`sum1`) OR (`v0`.`sum1`=`v0`.`cnt`)));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v4` (`c0`, `c1`, `c2`) AS SELECT 67.42 AS `c0`,`v0`.`g0` AS `c1`,_UTF8MB4'2026-03-19' AS `c2` FROM (`shiro_fuzz_r3`.`t0` LEFT JOIN `shiro_fuzz_r3`.`v0` ON (1=0)) JOIN `shiro_fuzz_r3`.`t1` ON (1=0) WHERE NOT EXISTS (SELECT `t1`.`id` AS `c0` FROM `shiro_fuzz_r3`.`t1` WHERE ((`t1`.`c0`<`t1`.`id`) AND (`t1`.`c0`<=>`t1`.`id`)));

SET FOREIGN_KEY_CHECKS=1;
