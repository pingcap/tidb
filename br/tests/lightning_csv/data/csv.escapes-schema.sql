/*

insert into `escapes` values
(1, '\\', '{"?":[]}', X'ffffffff'),
(2, '"', '"\\n\\n\\n"', X'0d0a0d0a'),
(3, '\n', '[",,,"]', '\\,\\,');

*/

create table `escapes` (
    `i` int primary key,
    `t` text,
    `j` json,
    `b` blob
);

