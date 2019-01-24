FROM hseeberger/scala-sbt:8u181_2.12.6_1.2.3

ADD . /rdb-allconnector

WORKDIR /rdb-allconnector

RUN apt-get update && apt-get install -y lsb-release

RUN wget https://dev.mysql.com/get/mysql-apt-config_0.8.10-1_all.deb && dpkg -i mysql-apt-config* && apt-get update && apt-get install -y mysql-client

RUN sbt clean compile
