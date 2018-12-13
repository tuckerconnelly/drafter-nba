env | grep RAMBLER

printf 'Waiting for postgres'
retries=10
until psql "postgres://$RAMBLER_USER:$RAMBLER_PASSWORD@$RAMBLER_HOST:$RAMBLER_PORT/$RAMBLER_DATABASE" -c 'select * from information_schema.tables' > /dev/null 2>&1 || [ $retries -eq 0 ]; do
  printf '.'
  sleep 1
  retries=$((retries - 1))
done
printf '\n'

rambler --debug apply -a
