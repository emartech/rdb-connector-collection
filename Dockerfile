FROM hseeberger/scala-sbt:8u181_2.12.6_1.2.3

ADD build.sbt /rdb-allconnector/build.sbt
ADD project /rdb-allconnector/project
ADD common /rdb-allconnector/common
ADD test /rdb-allconnector/test
ADD mysql /rdb-allconnector/mysql
ADD run_mysql_it_tests.sh /rdb-allconnector/run_mysql_it_tests.sh

WORKDIR /rdb-allconnector

RUN apt-get update && apt-get install -y mysql-client

RUN sbt clean compile
