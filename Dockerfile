FROM openlaw/scala-builder:0.9.3-alpine

ENV SBT_OPTS="${SBT_OPTS} -Dsbt.io.jdktimestamps=true -Xmx2G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xss2M"

WORKDIR /rdb-allconnector

RUN apk --no-cache add mysql-client openssl gnupg
ADD . .
RUN chmod +x run_mysql_it_tests.sh publish.sh

RUN sbt clean compile test:compile it:compile
