create table a (
    a varchar(20) primary key nonclustered,
    b int not null,
    c text,
    unique key uni_b(b)
);
