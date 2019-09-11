#!/usr/bin/env bash

count=0
function test_mysql {
  echo "Pinged mysql (${count})"
  mysqladmin ping -h "${DATABASE_HOST}" --silent
}

echo "Waiting for ${DATABASE_HOST} to become ready"

until ( test_mysql )
do
    ((count++))
    if [ ${count} -gt 1200 ]
    then
        echo "Services didn't become ready in time"
        exit 1
    fi
    sleep 0.1
done

sbt mysql/it:test
