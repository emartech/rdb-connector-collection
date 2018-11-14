FROM postgres:9.5

COPY postgresql.conf /usr/share/postgresql/9.5/postgresql.conf.sample
COPY ssl/server.crt ssl/server.key ssl/root.crt /var/ssl/
RUN chown postgres.postgres /usr/share/postgresql/9.5/postgresql.conf.sample \
                            /var/ssl/server.key \
                            /var/ssl/server.crt \
                            /var/ssl/root.crt && \
    chmod 600 /var/ssl/server.key

COPY initdb.d/init-unprivileged-user.sh /docker-entrypoint-initdb.d/init-unprivileged-user.sh
