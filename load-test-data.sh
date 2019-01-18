[[ -f ./api/.env ]] && . ./api/.env

# Postgres

# rm -r /usr/local/var/postgres && initdb /usr/local/var/postgres -U postgres -W

# Don't recreate the database in CI--it's already a fresh docker-compose build
# and run
if [[ -z "${CI}" ]]; then
  echo "select pg_terminate_backend(pid) from pg_stat_activity where datname='drafter_nba'" | psql 'postgres://postgres:password@localhost:5432/postgres'
  echo 'drop database drafter_nba' | psql 'postgres://postgres:password@localhost:5432/postgres'
  echo 'create database drafter_nba' | psql 'postgres://postgres:password@localhost:5432/postgres'
  cd rambler && rambler apply -a && cd ..
fi

# cat dump2.sql | psql 'postgres://postgres:password@localhost:5432/drafter_nba'

# psql 'postgres://postgres:password@localhost:5432/drafter_nba' << EOF
# -- Keep this idempotent by using ON CONFLICT DO NOTHING
# -- Try to rely on column defaults to be robust to schema changes.
# --
# -- ids on new tables are set to start 100, so feel free to use hard-coded ids
# -- up to that point
# EOF

# pg_dump 'postgresql://postgres:password@localhost:5432/drafter_nba' --data-only > dump.sql
