CREATE TABLE `q_alarm_group` (
`id` int(11) unsigned NOT NULL AUTO_INCREMENT,
`group_id` int(11) unsigned NOT NULL,
`name` varchar(255) NOT NULL,
`description` varchar(255) NOT NULL,
`send_mail` tinyint(4) unsigned NOT NULL,
`send_msg` tinyint(4) unsigned NOT NULL,
`send_time_from` tinyint(4) unsigned NOT NULL,
`send_time_to` tinyint(4) unsigned NOT NULL,
`receiver_ids` varchar(1024) NOT NULL,
`user_id` int(11) unsigned NOT NULL,
`status` tinyint(4) NOT NULL,
`created_at` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
`updated_at` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
