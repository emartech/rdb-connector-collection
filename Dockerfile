FROM hseeberger/scala-sbt:8u181_2.12.6_1.2.3

RUN apt-get update && apt-get install -y mysql-client

ADD . /rdb-allconnector

WORKDIR /rdb-allconnector
