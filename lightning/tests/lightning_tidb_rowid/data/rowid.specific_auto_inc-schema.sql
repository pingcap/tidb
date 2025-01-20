create table specific_auto_inc (
    a varchar(6) primary key /*T![clustered_index] NONCLUSTERED */,
    b int unique auto_increment) auto_increment=80000;
