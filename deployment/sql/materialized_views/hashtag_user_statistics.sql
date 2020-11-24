DROP MATERIALIZED VIEW IF EXISTS hashtag_user_statistics;
CREATE MATERIALIZED VIEW hashtag_user_statistics AS
  WITH general AS (
      SELECT
      user_id,
      hashtag_id,
      array_agg(id) changesets,
      max(coalesce(closed_at, created_at)) last_edit,
      count(*) changeset_count,
      sum(coalesce(total_edits, 0)) edit_count,
      max(updated_at) updated_at
    FROM changesets
    JOIN changesets_hashtags ON changesets.id = changesets_hashtags.changeset_id
    GROUP BY user_id, hashtag_id
  ),
  measurements AS (
    SELECT
      id,
      user_id,
      hashtag_id,
      key,
      value
    FROM changesets
    JOIN changesets_hashtags ON changesets.id = changesets_hashtags.changeset_id
    CROSS JOIN LATERAL jsonb_each(measurements)
  ),
  aggregated_measurements_kv AS (
    SELECT
      user_id,
      hashtag_id,
      key,
      sum((value->>0)::numeric) AS value
    FROM measurements
    GROUP BY user_id, hashtag_id, key
  ),
  aggregated_measurements AS (
    SELECT
      user_id,
      hashtag_id,
      jsonb_object_agg(key, value) measurements
    FROM aggregated_measurements_kv
    GROUP BY user_id, hashtag_id
  ),
  counts AS (
    SELECT
      id,
      user_id,
      hashtag_id,
      key,
      value
    FROM changesets
    JOIN changesets_hashtags ON changesets.id = changesets_hashtags.changeset_id
    CROSS JOIN LATERAL jsonb_each(counts)
  ),
  aggregated_counts_kv AS (
    SELECT
      user_id,
      hashtag_id,
      key,
      sum((value->>0)::numeric) AS value
    FROM counts
    GROUP BY user_id, hashtag_id, key
  ),
  aggregated_counts AS (
    SELECT
      user_id,
      hashtag_id,
      jsonb_object_agg(key, value) counts
    FROM aggregated_counts_kv
    GROUP BY user_id, hashtag_id
  )
  SELECT
    user_id,
    users.name,
    general.hashtag_id,
    hashtags.hashtag,
    measurements,
    counts,
    last_edit,
    changeset_count,
    edit_count,
    updated_at
  FROM general
  LEFT OUTER JOIN hashtags ON general.hashtag_id = hashtags.id
  LEFT OUTER JOIN aggregated_measurements USING (user_id, hashtag_id)
  LEFT OUTER JOIN aggregated_counts USING (user_id, hashtag_id)
  JOIN users ON user_id = users.id;

CREATE UNIQUE INDEX IF NOT EXISTS hashtag_user_statistics_pk ON hashtag_user_statistics(hashtag_id, user_id);