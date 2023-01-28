CREATE TABLE `t` (
     `id` int(11) NOT NULL,
     `name` varchar(255) DEFAULT NULL,
     PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
     UNIQUE KEY `uni_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
