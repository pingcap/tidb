SET FOREIGN_KEY_CHECKS=0;
DROP VIEW IF EXISTS v5;
DROP VIEW IF EXISTS v4;
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
  `c0` date NOT NULL,
  `c1` datetime NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_id_0` (`id`),
  KEY `idx_c0_4` (`c0`),
  KEY `idx_c1_15` (`c1`),
  KEY `idx_c1_c0_26` (`c1`,`c0`),
  KEY `idx_c0_c1_33` (`c0`,`c1`),
  CONSTRAINT `fk_350` FOREIGN KEY (`id`) REFERENCES `t2` (`id`),
  CONSTRAINT `fk_380` FOREIGN KEY (`id`) REFERENCES `t5` (`id`),
  CONSTRAINT `fk_434` FOREIGN KEY (`id`) REFERENCES `t3` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t1` (
  `id` bigint NOT NULL,
  `c0` tinyint(1) NOT NULL,
  `c1` tinyint(1) NOT NULL,
  `c2` float NOT NULL,
  `c3` timestamp NOT NULL,
  `c4` timestamp NOT NULL,
  `c5` decimal(12,2) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c0_1` (`c0`),
  KEY `idx_c1_11` (`c1`),
  KEY `idx_c4_16` (`c4`),
  KEY `idx_c5_21` (`c5`),
  KEY `idx_id_22` (`id`),
  KEY `idx_c0_c1_c3_23` (`c0`,`c1`,`c3`),
  KEY `idx_c2_23` (`c2`),
  KEY `idx_c3_24` (`c3`),
  KEY `idx_c0_c3_26` (`c0`,`c3`),
  KEY `idx_c4_c2_26` (`c4`,`c2`),
  KEY `idx_c5_c4_c2_28` (`c5`,`c4`,`c2`),
  KEY `idx_c1_c0_c5_28` (`c1`,`c0`,`c5`),
  KEY `idx_c4_c0_c1_32` (`c4`,`c0`,`c1`),
  KEY `idx_c0_c5_c3_35` (`c0`,`c5`,`c3`),
  KEY `idx_c0_c4_c5_35` (`c0`,`c4`,`c5`),
  KEY `idx_c2_c5_c0_35` (`c2`,`c5`,`c0`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY HASH (`id`) PARTITIONS 2;

CREATE TABLE `t2` (
  `id` bigint NOT NULL,
  `c0` datetime NOT NULL,
  `c1` decimal(12,2) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_id_8` (`id`),
  KEY `idx_c1_10` (`c1`),
  KEY `idx_c0_19` (`c0`),
  KEY `idx_c1_c0_23` (`c1`,`c0`),
  KEY `idx_c0_c1_27` (`c0`,`c1`),
  CONSTRAINT `fk_148` FOREIGN KEY (`id`) REFERENCES `t3` (`id`),
  CONSTRAINT `fk_149` FOREIGN KEY (`id`) REFERENCES `t0` (`id`),
  CONSTRAINT `fk_189` FOREIGN KEY (`id`) REFERENCES `t5` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t3` (
  `id` bigint NOT NULL,
  `c0` float NOT NULL,
  `c1` tinyint(1) NOT NULL,
  `c2` datetime DEFAULT NULL,
  `c3` datetime NOT NULL,
  `c4` int NOT NULL,
  `c5` tinyint(1) DEFAULT NULL,
  `c6` date NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c2` (`c2`),
  KEY `idx_c5` (`c5`),
  KEY `idx_c6` (`c6`),
  KEY `idx_c3_2` (`c3`),
  KEY `idx_c1_3` (`c1`),
  KEY `idx_id_6` (`id`),
  KEY `idx_c4_12` (`c4`),
  KEY `idx_c0_14` (`c0`),
  KEY `idx_c2_c3_c6_25` (`c2`,`c3`,`c6`),
  KEY `idx_c6_c3_c1_27` (`c6`,`c3`,`c1`),
  KEY `idx_c5_c1_c6_28` (`c5`,`c1`,`c6`),
  KEY `idx_c2_c0_c5_30` (`c2`,`c0`,`c5`),
  KEY `idx_c6_c2_33` (`c6`,`c2`),
  KEY `idx_c3_c6_c1_35` (`c3`,`c6`,`c1`),
  KEY `idx_c6_c5_c3_35` (`c6`,`c5`,`c3`),
  KEY `idx_c1_c6_35` (`c1`,`c6`),
  KEY `idx_c6_c5_35` (`c6`,`c5`),
  CONSTRAINT `fk_656` FOREIGN KEY (`id`) REFERENCES `t2` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t4` (
  `id` bigint NOT NULL,
  `c0` tinyint(1) DEFAULT NULL,
  `c1` varchar(64) NOT NULL,
  `c2` tinyint(1) DEFAULT NULL,
  `c3` varchar(64) NOT NULL,
  `c4` timestamp NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c4` (`c4`),
  KEY `idx_c2_13` (`c2`),
  KEY `idx_c1_17` (`c1`),
  KEY `idx_c0_18` (`c0`),
  KEY `idx_c3_20` (`c3`),
  KEY `idx_id_25` (`id`),
  KEY `idx_c4_c2_26` (`c4`,`c2`),
  KEY `idx_c2_c1_c0_27` (`c2`,`c1`,`c0`),
  KEY `idx_c4_c2_c3_27` (`c4`,`c2`,`c3`),
  KEY `idx_c1_c4_28` (`c1`,`c4`),
  KEY `idx_c0_c2_31` (`c0`,`c2`),
  KEY `idx_c4_c1_33` (`c4`,`c1`),
  KEY `idx_c0_c4_33` (`c0`,`c4`),
  KEY `idx_c3_c4_35` (`c3`,`c4`),
  KEY `idx_c2_c1_35` (`c2`,`c1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY HASH (`id`) PARTITIONS 4;

CREATE TABLE `t5` (
  `id` bigint NOT NULL,
  `c0` double NOT NULL,
  `c1` date NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c1_5` (`c1`),
  KEY `idx_c0_7` (`c0`),
  KEY `idx_id_9` (`id`),
  KEY `idx_c1_c0_23` (`c1`,`c0`),
  KEY `idx_c0_c1_28` (`c0`,`c1`),
  CONSTRAINT `fk_18` FOREIGN KEY (`id`) REFERENCES `t2` (`id`),
  CONSTRAINT `fk_30` FOREIGN KEY (`id`) REFERENCES `t3` (`id`),
  CONSTRAINT `fk_67` FOREIGN KEY (`id`) REFERENCES `t0` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v0` (`c0`) AS SELECT DISTINCT (`t0`.`id`-65) AS `c0` FROM `shiro_fuzz_r5`.`t0` LEFT JOIN `shiro_fuzz_r5`.`t1` ON (1=0) WHERE NOT EXISTS (SELECT `t1`.`c4` AS `c0` FROM `shiro_fuzz_r5`.`t1` WHERE ((`t1`.`c0`>`t1`.`c1`) AND (`t1`.`c4`>`t1`.`c3`)));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v1` (`c0`, `c1`, `c2`) AS SELECT (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r5`.`v0` WHERE (`v0`.`c0`=`t0`.`c1`)) AS `c0`,SUM(`t1`.`c2`) OVER (ORDER BY `t1`.`c4`,`v0`.`c0`) AS `c1`,`t1`.`c0` AS `c2` FROM (`shiro_fuzz_r5`.`v0` RIGHT JOIN `shiro_fuzz_r5`.`t1` ON (1=0)) LEFT JOIN `shiro_fuzz_r5`.`t0` ON (1=0) WHERE EXISTS (SELECT `v0`.`c0` AS `c0` FROM `shiro_fuzz_r5`.`v0` WHERE ((_UTF8MB4'2025-02-23 19:48:48'=_UTF8MB4'2026-08-14 14:28:36') OR (`v0`.`c0`<=_UTF8MB4's49')) LIMIT 3);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v2` (`c0`, `c1`, `c2`) AS SELECT 0 AS `c0`,(SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r5`.`t1` WHERE (`t1`.`id`=`t0`.`id`)) AS `c1`,86.2 AS `c2` FROM (`shiro_fuzz_r5`.`t0` RIGHT JOIN `shiro_fuzz_r5`.`v0` ON (1=0)) LEFT JOIN `shiro_fuzz_r5`.`t1` ON (1=0) WHERE (`t0`.`c0`<=>`t0`.`c1`);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v4` (`c0`) AS SELECT (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r5`.`v0` WHERE (`v0`.`c0`=`v0`.`c0`) ORDER BY COUNT(1) LIMIT 3) AS `c0` FROM (`shiro_fuzz_r5`.`v0` LEFT JOIN `shiro_fuzz_r5`.`t1` ON (1=0)) LEFT JOIN `shiro_fuzz_r5`.`t0` ON (1=0) WHERE ((NOT (`t1`.`c4` IN (_UTF8MB4'2026-07-15 01:47:18',_UTF8MB4'2026-10-22 06:43:27',_UTF8MB4'2026-02-23 14:11:01')) AND EXISTS (SELECT `v1`.`c0` AS `c0` FROM `shiro_fuzz_r5`.`v1` WHERE ((`v1`.`c1`=`v1`.`c0`) OR (`v1`.`c0`>`v1`.`c1`)))) OR (`t1`.`id`>`t0`.`id`));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v5` (`c0`) AS SELECT `v0`.`c0` AS `c0` FROM ((`shiro_fuzz_r5`.`t0` JOIN `shiro_fuzz_r5`.`v0` ON (1=0)) LEFT JOIN `shiro_fuzz_r5`.`t1` ON (1=0)) LEFT JOIN `shiro_fuzz_r5`.`v4` ON (1=0) WHERE (NOT (`t1`.`c4` IN (_UTF8MB4'2024-09-20 13:58:50',_UTF8MB4'2025-10-02 18:57:55',_UTF8MB4'2025-09-28 02:15:41')) OR NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r5`.`v2` WHERE (`v2`.`c0`>`v2`.`c2`))) ORDER BY `t1`.`c2` DESC LIMIT 14;

SET FOREIGN_KEY_CHECKS=1;
