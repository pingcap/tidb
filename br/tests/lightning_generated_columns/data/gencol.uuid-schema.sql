CREATE TABLE `uuid` (
  `a` bigint(30) NOT NULL,
  `b` varchar(40) NOT NULL DEFAULT uuid(),
  UNIQUE KEY `c` (`b`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;