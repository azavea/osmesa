DROP MATERIALIZED VIEW IF EXISTS country_statistics;
CREATE MATERIALIZED VIEW country_statistics AS
  WITH changesets AS (
    SELECT
      *
    FROM changesets
    -- ignore users 0 and 1
    WHERE user_id > 1
  ),
  general AS (
    SELECT
      country_id,
      max(coalesce(closed_at, created_at)) last_edit,
      count(*) changeset_count,
      sum(coalesce(edit_count, 0)) edit_count,
      max(updated_at) updated_at
    FROM changesets
    JOIN changesets_countries ON changesets.id = changesets_countries.changeset_id
    GROUP BY country_id
  ),
  processed_changesets AS (
    SELECT
      id,
      user_id,
      country_id,
      measurements,
      counts,
      edit_count
    FROM changesets
    JOIN changesets_countries ON changesets.id = changesets_countries.changeset_id
  ),
  hashtag_counts AS (
    SELECT
      RANK() OVER (PARTITION BY country_id ORDER BY sum(coalesce(edit_count, 0)) DESC) AS rank,
      country_id,
      hashtag,
      count(*) changesets,
      sum(coalesce(edit_count, 0)) edits
    FROM processed_changesets
    JOIN changesets_hashtags ON processed_changesets.id = changesets_hashtags.changeset_id
    JOIN hashtags ON changesets_hashtags.hashtag_id = hashtags.id
    GROUP BY country_id, hashtag
  ),
  hashtags AS (
    SELECT
      country_id,
      jsonb_object_agg(hashtag, changesets) hashtag_changesets,
      jsonb_object_agg(hashtag, edits) hashtag_edits
    FROM hashtag_counts
    WHERE rank <= 10
    GROUP BY country_id
  ),
  user_counts AS (
    SELECT
      RANK() OVER (PARTITION BY country_id ORDER BY sum(coalesce(edit_count, 0)) DESC) AS rank,
      country_id,
      user_id,
      count(*) changesets,
      sum(coalesce(edit_count, 0)) edits
    FROM processed_changesets
    GROUP BY country_id, user_id
  ),
  users AS (
    SELECT
      country_id,
      jsonb_object_agg(user_id, changesets) user_changesets,
      jsonb_object_agg(user_id, edits) user_edits
    FROM user_counts
    WHERE rank <= 10
    GROUP BY country_id
  ),
  measurements AS (
    SELECT
      id,
      country_id,
      key,
      value
    FROM processed_changesets
    CROSS JOIN LATERAL jsonb_each(measurements)
  ),
  aggregated_measurements_kv AS (
    SELECT
      country_id,
      key,
      sum((value->>0)::numeric) AS value
    FROM measurements
    GROUP BY country_id, key
  ),
  aggregated_measurements AS (
    SELECT
      country_id,
      jsonb_object_agg(key, value) measurements
    FROM aggregated_measurements_kv
    GROUP BY country_id
  ),
  counts AS (
    SELECT
      id,
      country_id,
      key,
      value
    FROM processed_changesets
    CROSS JOIN LATERAL jsonb_each(counts)
  ),
  aggregated_counts_kv AS (
    SELECT
      country_id,
      key,
      sum((value->>0)::numeric) AS value
    FROM counts
    GROUP BY country_id, key
  ),
  aggregated_counts AS (
    SELECT
      country_id,
      jsonb_object_agg(key, value) counts
    FROM aggregated_counts_kv
    GROUP BY country_id
  )
  SELECT
    general.country_id,
    countries.name country_name,
    countries.code country_code,
    -- NOTE these are per-changeset, not per-country, so stats are double-counted
    measurements,
    -- NOTE these are per-changeset, not per-country, so stats are double-counted
    counts,
    general.changeset_count,
    general.edit_count,
    general.last_edit,
    general.updated_at,
    user_changesets,
    user_edits,
    hashtag_changesets,
    hashtag_edits
  FROM general
  JOIN countries ON country_id = countries.id
  LEFT OUTER JOIN users USING (country_id)
  LEFT OUTER JOIN hashtags USING (country_id)
  LEFT OUTER JOIN aggregated_measurements USING (country_id)
  LEFT OUTER JOIN aggregated_counts USING (country_id);

CREATE UNIQUE INDEX IF NOT EXISTS country_statistics_id ON country_statistics(country_code);
