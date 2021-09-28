create table c (
    a double not null,
    b decimal not null,
    c text,
    primary key (a, b) clustered,
    unique key (b, c(10))
);
