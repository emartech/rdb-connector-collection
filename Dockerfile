FROM openlaw/scala-builder:0.9.3-alpine

ENV SBT_OPTS="${SBT_OPTS} -Dsbt.io.jdktimestamps=true -Xmx2G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xss2M"

WORKDIR /rdb-allconnector

# Openssl is needed to decrypt secret.
# Gnupg is needed for signing.
# Git is need for dynver for proper versoning.
RUN apk --no-cache add openssl gnupg git
ADD . .
RUN chmod +x run_mysql_it_tests.sh publish.sh

RUN sbt clean compile test:compile it:compile
