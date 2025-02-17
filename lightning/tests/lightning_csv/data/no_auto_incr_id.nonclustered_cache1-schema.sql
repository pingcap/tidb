CREATE TABLE `nonclustered_cache1` (
    `id` int(11) NOT NULL,
    v int,
    PRIMARY KEY(`id`) NONCLUSTERED
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_ID_CACHE 1;
