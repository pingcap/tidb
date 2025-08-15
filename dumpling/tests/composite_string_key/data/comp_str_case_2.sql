# test composite string primary key - three column composite key with mixed length strings
create table `comp_str_case_2` (
  region varchar(20),
  country varchar(50),
  city varchar(100),
  population bigint,
  area_km2 decimal(15,2),
  primary key (region, country, city)
);

insert into `comp_str_case_2` values
('asia', 'china', 'beijing', 21540000, 16410.54),
('asia', 'china', 'shanghai', 24280000, 6340.50),
('asia', 'japan', 'tokyo', 37400000, 2194.07),
('asia', 'japan', 'osaka', 19281000, 1905.00),
('europe', 'france', 'paris', 10858000, 105.40),
('europe', 'germany', 'berlin', 3669000, 891.85),
('europe', 'italy', 'rome', 2873000, 1285.31),
('north america', 'usa', 'new york', 8336000, 778.20),
('north america', 'usa', 'los angeles', 3979000, 1302.15),
('north america', 'canada', 'toronto', 2930000, 630.21),
('south america', 'brazil', 'sao paulo', 12330000, 1521.11),
('oceania', 'australia', 'sydney', 5312000, 12368.19);