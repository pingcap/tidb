CREATE TABLE `q_report_summary` (
`id` int(11) unsigned NOT NULL AUTO_INCREMENT,
`group_id` int(11) unsigned NOT NULL DEFAULT '0',
`host_count` int(11) unsigned NOT NULL DEFAULT '0',
`circular_count` int(11) unsigned NOT NULL DEFAULT '0',
`active_count` int(11) unsigned NOT NULL DEFAULT '0',
`security_num` int(11) unsigned NOT NULL DEFAULT '0',
`hole_num` int(11) unsigned NOT NULL DEFAULT '0',
`day` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
`created_at` timestamp NULL DEFAULT NULL,
PRIMARY KEY (`id`),
KEY `group_id` (`group_id`) USING BTREE,
KEY `day` (`day`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;