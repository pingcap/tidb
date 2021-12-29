/*!40101 SET NAMES binary*/;
<<<<<<< HEAD
=======
/*T![placement] SET PLACEMENT_CHECKS = 0*/;
>>>>>>> 1e7f0dcc6... dumpling: add collation_compatible config in dumpling (#31114)
CREATE TABLE `quo``te/table` (
  `quo``te/col` int(11) NOT NULL,
  `a` int(11) DEFAULT NULL,
  `gen``id` int(11) GENERATED ALWAYS AS (`quo``te/col`) VIRTUAL,
  PRIMARY KEY (`quo``te/col`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
