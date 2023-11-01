create table a (
    a int primary key nonclustered,
    b int not null,
    c int not null,
    d text,
    key key_b(b),
    key key_c(c)
);
