#!/usr/bin/env bash

echo "$(date -Iseconds): Starting view refreshment in $DATABASE_NAME"

if [ "$(psql -Aqtc "select count(pid) from pg_stat_activity where query ilike 'refresh materialized view concurrently user_statistics%' and state='active' and datname='$DATABASE_NAME'" $DATABASE_URL 2> /dev/null)" == "0" ]; then
  echo "$(date -Iseconds): Refreshing user statistics"
  # refresh in the background to return immediately
  psql -Aqt \
    -c "REFRESH MATERIALIZED VIEW CONCURRENTLY user_statistics" \
    -c "UPDATE refreshments SET updated_at=now() where mat_view='user_statistics'" \
    $DATABASE_URL &
else
  echo "$(date -Iseconds): User stats table already refreshing"
fi

if [ "$(psql -Aqtc "select count(pid) from pg_stat_activity where query ilike 'refresh materialized view concurrently hashtag_statistics%' and state='active' and datname='$DATABASE_NAME'" $DATABASE_URL 2> /dev/null)" == "0" ]; then
  echo "$(date -Iseconds): Refreshing hashtag statistics"
  # refresh in the background to return immediately
  psql -Aqt \
    -c "REFRESH MATERIALIZED VIEW CONCURRENTLY hashtag_statistics" \
    -c "UPDATE refreshments SET updated_at=now() where mat_view='hashtag_statistics'" \
    $DATABASE_URL &
else
  echo "$(date -Iseconds): Hashtag stats table already refreshing"
fi

if [ "$(psql -Aqtc "select count(pid) from pg_stat_activity where query ilike 'refresh materialized view concurrently country_statistics%' and state='active' and datname='$DATABASE_NAME'" $DATABASE_URL 2> /dev/null)" == "0" ]; then
  # refresh in the background to return immediately
  echo "$(date -Iseconds): Refreshing country statistics"
  psql -Aqt \
    -c "REFRESH MATERIALIZED VIEW CONCURRENTLY country_statistics" \
    -c "UPDATE refreshments SET updated_at=now() where mat_view='country_statistics'" \
    $DATABASE_URL &
else
  echo "$(date -Iseconds): Country stats table already refreshing"
fi

if [ "$(psql -Aqtc "select count(pid) from pg_stat_activity where query ilike 'refresh materialized view concurrently hashtag_user_statistics%' and state='active' and datname='$DATABASE_NAME'" $DATABASE_URL 2> /dev/null)" == "0" ]; then
  # refresh in the background to return immediately
  echo "$(date -Iseconds): Refreshing hashtag/user statistics"
  psql -Aqt \
    -c "REFRESH MATERIALIZED VIEW CONCURRENTLY hashtag_user_statistics" \
    -c "UPDATE refreshments SET updated_at=now() where mat_view='hashtag_user_statistics'" \
    $DATABASE_URL &
else
  echo "$(date -Iseconds): Hashtag/user stats table already refreshing"
fi

wait
echo "$(date -Iseconds): Completed"
