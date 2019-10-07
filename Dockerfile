FROM openlaw/scala-builder:0.9.3-alpine

ENV SBT_OPTS="${SBT_OPTS} -Dsbt.io.jdktimestamps=true -Xmx2G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xss2M"

WORKDIR /rdb-allconnector

# Openssl is needed to decrypt secret.
# Gnupg is needed for signing.
# Git is need for dynver for proper versoning.
RUN apk --no-cache add openssl gnupg git

COPY ci/secrets.tar.enc ci/secrets.tar.enc
COPY publish.sh publish.sh
RUN chmod +x publish.sh

COPY build.sbt build.sbt
ADD project project
RUN sbt update

ADD bigquery bigquery
ADD mssql mssql
ADD redshift redshift
ADD test test
ADD common common
ADD postgresql postgresql
ADD mysql mysql

RUN sbt clean compile test:compile it:compile
