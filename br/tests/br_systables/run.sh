#! /bin/bash

set -eux

backup_dir=$TEST_DIR/$TEST_NAME

test_data="('TiDB'),('TiKV'),('TiFlash'),('TiSpark'),('TiCDC'),('TiPB'),('Rust'),('C++'),('Go'),('Haskell'),('Scala')"

modify_systables() {
    run_sql "CREATE USER 'Alyssa P. Hacker'@'%' IDENTIFIED BY 'password';"
    run_sql "UPDATE mysql.tidb SET VARIABLE_VALUE = '1h' WHERE VARIABLE_NAME = 'tikv_gc_life_time';"

    run_sql "CREATE TABLE mysql.foo(pk int primary key auto_increment, field varchar(255));"
    run_sql "CREATE TABLE mysql.bar(pk int primary key auto_increment, field varchar(255));"

    run_sql "INSERT INTO mysql.foo(field) VALUES $test_data"
    run_sql "INSERT INTO mysql.bar(field) VALUES $test_data"

    go-ycsb load mysql -P tests/"$TEST_NAME"/workload \
        -p mysql.host="$TIDB_IP" \
        -p mysql.port="$TIDB_PORT" \
        -p mysql.user=root \
        -p mysql.db=mysql

    run_sql "ANALYZE TABLE mysql.usertable;"
}

add_user() {
    run_sql "CREATE USER 'newuser' IDENTIFIED BY 'newuserpassword';"
}

delete_user() {
    # FIXME don't check the user table until we support restore user correctly.
    echo "delete user newuser"
    # run_sql "DROP USER 'newuser'"
}

add_test_data() {
    run_sql "CREATE DATABASE usertest;"
    run_sql "CREATE TABLE usertest.test(pk int primary key auto_increment, field varchar(255));"
    run_sql "INSERT INTO usertest.test(field) VALUES $test_data"
}

delete_test_data() {
    run_sql "DROP TABLE usertest.test;"
}

rollback_modify() {
    run_sql "DROP TABLE mysql.foo;"
    run_sql "DROP TABLE mysql.bar;"
    run_sql "UPDATE mysql.tidb SET VARIABLE_VALUE = '10m' WHERE VARIABLE_NAME = 'tikv_gc_life_time';"
    # FIXME don't check the user table until we support restore user correctly.
    # run_sql "DROP USER 'Alyssa P. Hacker';"
    run_sql "DROP TABLE mysql.usertable;"
}

check() {
    run_sql "SELECT count(*) from mysql.foo;" | grep 11
    run_sql "SELECT count(*) from mysql.usertable;" | grep 1000
    run_sql "SHOW TABLES IN mysql;" | awk '/bar/{exit 1}'
    # we cannot let user overwrite `mysql.tidb` through br in any time.
    run_sql "SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'" | awk '/1h/{exit 1}'

    # FIXME don't check the user table until we support restore user correctly.
    # TODO remove this after supporting auto flush.
    # run_sql "FLUSH PRIVILEGES;"
    # run_sql "SELECT CURRENT_USER();" -u'Alyssa P. Hacker' -p'password' | grep 'Alyssa P. Hacker'
    # run_sql "SHOW DATABASES" | grep -v '__TiDB_BR_Temporary_'
    # TODO check stats after supportting.
}

check2() {
    run_sql "SELECT count(*) from usertest.test;" | grep 11
    # FIXME don't check the user table until we support restore user correctly.
    # run_sql "SELECT user FROM mysql.user WHERE user='newuser';" | grep 'newuser'
}

modify_systables
run_br backup full -s "local://$backup_dir"
rollback_modify
run_br restore full -f '*.*' -f '!mysql.bar' -s "local://$backup_dir"
check

run_br restore full -f 'mysql.bar' -s "local://$backup_dir"
run_sql "SELECT count(*) from mysql.bar;" | grep 11

rollback_modify 
run_br restore full -f "mysql*.*" -f '!mysql.bar' -s "local://$backup_dir"
check

add_user
add_test_data
run_br backup full -s "local://${backup_dir}1"
delete_user
delete_test_data
run_br restore full -f "mysql*.*" -f "usertest.*" -s "local://${backup_dir}1"
check2

delete_user 
run_br restore db --db mysql -s "local://${backup_dir}1"
check2

