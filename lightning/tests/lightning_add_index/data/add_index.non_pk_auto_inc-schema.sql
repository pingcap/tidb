CREATE TABLE `non_pk_auto_inc` (
  `pk` varchar(255) PRIMARY KEY NONCLUSTERED,
  `id` int(11) NOT NULL AUTO_INCREMENT,
  UNIQUE KEY uniq_id (`id`)
);
