create table a (
    a int primary key clustered,
    b int not null,
    c text,
    unique key uni_b(b)
);
