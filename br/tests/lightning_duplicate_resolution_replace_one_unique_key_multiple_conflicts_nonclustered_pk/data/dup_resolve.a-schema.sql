create table a (
    a int primary key nonclustered,
    b int not null,
    c text,
    unique key uni_b(b)
);
