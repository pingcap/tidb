CREATE TABLE `clustered_cache1` (
    `id` int(11) NOT NULL,
    v int,
    PRIMARY KEY(`id`) CLUSTERED
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_ID_CACHE 1;
