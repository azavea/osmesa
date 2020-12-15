#!/usr/bin/env bash
# Send email if ChangesetStatsUpdater is at least OFFSET_THRESHOLD minutes behind.
# Requires that `mailx` be installed.
#
# Ensure the following env variables are set:
#   - DATABASE_URL: A valid postgres connection string
#   - ENVIRONMENT: A unique string describing the environment, usually "staging"|"production"
#   - FROM_EMAIL: Email address to send alert from
#   - TO_EMAIL: Email address to send alert to
#   - SMTP_HOSTNAME: Hostname of SMTP server to send mail to
# Optional:
#   - OFFSET_THRESHOLD: Default 10. Offset in minutes to begin alerting at.
#   - SMTP_PORT: Default 25

set -e

CHANGESET_CHECKPOINT=$(psql -Aqtc "select sequence from checkpoints where proc_name = 'ChangesetStatsUpdater'" $DATABASE_URL)
EPOCH_NOW=$(date +%s)
ADIFF_SEQUENCE_NOW=$(( (${EPOCH_NOW} - 1347432900) / 60 ))

OFFSET=$(( ${ADIFF_SEQUENCE_NOW} - ${CHANGESET_CHECKPOINT} ))

if (( ${OFFSET} >= ${OFFSET_THRESHOLD:-10} )); then
    echo "OSMesa ChangesetStatsUpdater in ${ENVIRONMENT} is behind by ${OFFSET}" | \
      mailx \
        -s "ALERT: OSMesa ChangesetStats Slow (${ENVIRONMENT})" \
        -S smtp=smtp://${SMTP_HOSTNAME}:${SMTP_PORT:-25} \
        -S from="${FROM_EMAIL}" \
        "${TO_EMAIL}"
fi
