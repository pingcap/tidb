-- https://github.com/pingcap/br/issues/1404
-- expression indices just use a hidden virtual generated column behind the scene.

CREATE TABLE `expr_index` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `a` varchar(20) DEFAULT NULL,
    `b` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
    KEY `idx_a` (`a`),
    KEY `idx_lower_b` ((lower(`b`)))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=90003;
