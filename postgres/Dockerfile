######################## BUILDER #########################

FROM postgres:13 AS builder

RUN apt-get update -y \
    && apt-get install -y gcc make postgresql-server-dev-13 git \
    && git clone https://github.com/eulerto/wal2json.git

WORKDIR /wal2json

RUN USE_PGXS=1 make && make install

######################### FINAL #########################

FROM postgres:13

COPY ./postgresql.conf /etc/postgresql/postgresql.conf
COPY ./pg_hba.conf /etc/postgresql/pg_hba.conf
COPY --from=builder /wal2json/wal2json.so /usr/lib/postgresql/13/lib/wal2json.so

RUN chown -R postgres /etc/postgresql

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]

# https://forums.docker.com/t/configuration-file-contains-errors/118959/2