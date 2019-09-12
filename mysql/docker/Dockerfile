ARG MYSQL_VERSION

FROM mysql/mysql-server:$MYSQL_VERSION

COPY ssl etc/mysql
COPY my.cnf etc/my.cnf

HEALTHCHECK --interval=1s --timeout=1s --retries=120 CMD /healthcheck.sh
