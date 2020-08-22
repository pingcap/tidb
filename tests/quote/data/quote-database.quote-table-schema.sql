CREATE TABLE `quo``te/table` (
  `quo``te/col` int(11) NOT NULL,
  `a` int(11) DEFAULT NULL,
  `gen``id` int(11) GENERATED ALWAYS AS (`quo``te/col`) VIRTUAL,
  PRIMARY KEY (`quo``te/col`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
