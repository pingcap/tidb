SET FOREIGN_KEY_CHECKS=0;
DROP VIEW IF EXISTS v1;
DROP VIEW IF EXISTS v0;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t0;
CREATE TABLE `t0` (
  `id` bigint NOT NULL,
  `c0` datetime NOT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` int NOT NULL,
  `c3` decimal(12,2) DEFAULT NULL,
  `c4` tinyint(1) NOT NULL,
  `c5` bigint NOT NULL,
  `c6` date NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c1` (`c1`),
  KEY `idx_c2` (`c2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t1` (
  `id` bigint NOT NULL,
  `c0` int NOT NULL,
  `c1` int NOT NULL,
  `c2` int NOT NULL,
  `c3` bigint NOT NULL,
  `c4` float NOT NULL,
  `c5` double NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c2` (`c2`),
  KEY `idx_c3` (`c3`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v0` (`c0`) AS SELECT 19 AS `c0` FROM `shiro_fuzz_r1`.`t0` LEFT JOIN `shiro_fuzz_r1`.`t1` ON (1=0) WHERE ((NOT EXISTS (SELECT `t0`.`c5` AS `c0` FROM `shiro_fuzz_r1`.`t0` WHERE ((`t0`.`c2`!=`t0`.`c1`) AND (`t0`.`c5`!=`t0`.`c1`))) AND NOT EXISTS (SELECT `t0`.`id` AS `c0` FROM `shiro_fuzz_r1`.`t0` WHERE ((`t0`.`c2`>`t0`.`c5`) AND (`t0`.`c2`<=>`t0`.`c1`)) ORDER BY `t0`.`c1`,`t0`.`c5` LIMIT 5)) AND (EXISTS (SELECT `t0`.`c1` AS `c0` FROM `shiro_fuzz_r1`.`t0` WHERE (`t0`.`c2`!=`t0`.`c1`) ORDER BY `t0`.`c3` DESC,`t0`.`c2` DESC LIMIT 8) OR EXISTS (SELECT `t0`.`c2` AS `c0` FROM `shiro_fuzz_r1`.`t0` WHERE (`t0`.`c0`=`t0`.`c6`) ORDER BY (`t0`.`c3`*`t0`.`c2`) DESC LIMIT 5))) ORDER BY `t0`.`c4`,(`t1`.`c5`*`t1`.`c1`) DESC LIMIT 11;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v1` (`c0`, `c1`, `c2`) AS SELECT 24.49 AS `c0`,AVG(`t0`.`c3`) OVER (ORDER BY `v0`.`c0` DESC) AS `c1`,`t0`.`c2` AS `c2` FROM (`shiro_fuzz_r1`.`t0` LEFT JOIN `shiro_fuzz_r1`.`v0` ON (1=0)) RIGHT JOIN `shiro_fuzz_r1`.`t1` ON (1=0) WHERE (`t0`.`c3`<`t0`.`c1`) ORDER BY `t0`.`c1` DESC,`t0`.`c4` LIMIT 15;

SET FOREIGN_KEY_CHECKS=1;
