/*!40101 SET NAMES binary*/;
<<<<<<< HEAD
=======
/*T![placement] SET PLACEMENT_CHECKS = 0*/;
>>>>>>> 1e7f0dcc6... dumpling: add collation_compatible config in dumpling (#31114)
CREATE TABLE `quo``te/table` (
  `quo``te/col` int NOT NULL,
  `a` int DEFAULT NULL,
  `gen``id` int GENERATED ALWAYS AS (`quo``te/col`) VIRTUAL,
  PRIMARY KEY (`quo``te/col`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
