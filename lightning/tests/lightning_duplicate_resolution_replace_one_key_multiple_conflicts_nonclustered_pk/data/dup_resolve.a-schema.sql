create table a (
    a int primary key nonclustered,
    b int not null,
    c text,
    key key_b(b)
);
