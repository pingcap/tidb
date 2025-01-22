create table a (
    a int primary key nonclustered,
    b int not null,
    c int not null,
    d text,
    unique key uni_b(b),
    unique key uni_c(c)
);
