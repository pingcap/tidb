CREATE TABLE `q_fish_event` (
`id` int(11) unsigned NOT NULL AUTO_INCREMENT,
`host_id` int(11) unsigned NOT NULL,
`group_id` int(11) unsigned NOT NULL,
`reason` varchar(1000) NOT NULL DEFAULT '',
`affected_url` varchar(1000) NOT NULL DEFAULT '',
`fish_id` int(11) unsigned NOT NULL,
`level` tinyint(1) NOT NULL DEFAULT '2',
`created_at` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
`updated_at` timestamp NULL DEFAULT NULL,
`status` tinyint(1) NOT NULL DEFAULT '0',
PRIMARY KEY (`id`),
KEY `created_at` (`created_at`) USING BTREE,
KEY `idx_event_id` (`host_id`,`group_id`,`created_at`)
) ENGINE=InnoDB AUTO_INCREMENT=8343230 DEFAULT CHARSET=utf8;