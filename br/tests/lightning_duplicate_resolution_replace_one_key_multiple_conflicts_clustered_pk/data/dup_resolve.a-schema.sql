create table a (
    a int primary key clustered,
    b int not null,
    c text,
    key key_b(b)
);
