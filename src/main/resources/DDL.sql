CREATE TABLE `test_table2` (
  `id` varchar(50) NOT NULL DEFAULT '',
  `created_at` text,
  `body` text,
  `sentiment` text,
  `followers` text,
  `like_count` text,
  `following` text,
  `ideas` text,
  `symbols` text,
  `username` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
