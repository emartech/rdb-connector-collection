#!/usr/bin/env bash

count=0

function test_mysql {
  echo "Pinged mysql (${count +1 })"
  mysqladmin ping -h mysql-db-57 --silent
}

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
