FROM hseeberger/scala-sbt:8u181_2.12.6_1.2.3

ADD . /rdb-allconnector

WORKDIR /rdb-allconnector

RUN apt-get update && apt-get install -y mysql-client

RUN sbt clean compile
