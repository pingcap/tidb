CREATE TABLE `q_config` (
`id` int(11) unsigned NOT NULL AUTO_INCREMENT,
`key` varchar(255) NOT NULL,
`value` varchar(255) NOT NULL,
`type` tinyint(4) NOT NULL,
`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
`updated_at` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;