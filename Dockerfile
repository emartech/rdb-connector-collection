FROM hseeberger/scala-sbt:8u212_1.2.8_2.12.8

RUN apt-get update && apt-get install -y mysql-client

ADD . /rdb-allconnector

WORKDIR /rdb-allconnector
