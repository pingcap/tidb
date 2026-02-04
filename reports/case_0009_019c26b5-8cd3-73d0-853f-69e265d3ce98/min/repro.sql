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
CREATE TABLE `t4` (
  `id` bigint NOT NULL,
  `k3` int NOT NULL,
  `k0` int NOT NULL,
  `d0` float NOT NULL,
  `d1` int NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
INSERT INTO `t2` (`id`, `k1`, `k0`, `d0`, `d1`) VALUES (9, 73, -4, 0, -68), (10, 47, -20, 0, 41), (5, -8, -61, 1, -38), (7, 81, 11, 0, 58), (1, 16, 87, 1, 15), (2, 45, -61, 1, -9), (3, 91, -20, 0, -42), (4, 90, 67, 1, 16), (6, -40, 14, 0, -72), (8, 12, -12, 0, 41);
INSERT INTO `t4` (`id`, `k3`, `k0`, `d0`, `d1`) VALUES (9, 13, 67, 12.41, -68), (1, 70, 49, 21.21, 22), (3, -17, -61, 61.69, 36), (10, 25, 11, 18.92, -88), (2, -44, -12, 90.69, 20), (4, -26, 49, 26.48, 94), (5, 66, 49, 57.77, 29), (6, -71, -12, 4.63, -67), (7, -31, 67, 36.15, -99), (8, 51, -20, 21.45, -59);
WITH cte_0 AS (SELECT t2.d0 AS c0, t2.id AS c1, t2.d1 AS c2 FROM t2 WHERE (t2.id < t2.k0) ORDER BY t2.id LIMIT 2) SELECT AVG(cte_0.c1) OVER (ORDER BY v2.c0 DESC) AS c0, cte_0.c2 AS c1, v1.c1 AS c2 FROM v2 RIGHT JOIN v3 ON (1 = 0) LEFT JOIN t2 ON (1 = 0) RIGHT JOIN v4 ON (1 = 0) JOIN t4 ON (t2.k0 = t4.k0) RIGHT JOIN v1 ON (1 = 0) RIGHT JOIN cte_0 ON (1 = 0) WHERE EXISTS (SELECT COUNT(1) AS cnt FROM v3 WHERE EXISTS (SELECT v3.sum1 AS c0 FROM v3 WHERE ((v3.sum1 = v3.cnt) AND (v3.g0 <= v3.sum1))) ORDER BY COUNT(1) DESC LIMIT 7) ORDER BY LENGTH(v2.c2) DESC LIMIT 15;
