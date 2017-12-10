#!/bin/sh
while :
do
    sleep 3
    if mysql -e 'select version()'; then
        break
    fi
done
