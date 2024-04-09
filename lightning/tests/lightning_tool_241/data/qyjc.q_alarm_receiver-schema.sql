CREATE TABLE `q_alarm_receiver` (
`id` int(11) unsigned NOT NULL AUTO_INCREMENT,
`name` varchar(255) NOT NULL,
`email` varchar(255) NOT NULL,
`phone_number` char(50) NOT NULL,
`receiver_id` int(11) unsigned NOT NULL,
`user_id` int(11) unsigned NOT NULL,
`status` tinyint(4) unsigned NOT NULL,
`created_at` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
`updated_at` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;