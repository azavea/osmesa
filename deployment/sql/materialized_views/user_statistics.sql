DROP MATERIALIZED VIEW IF EXISTS user_statistics;
CREATE MATERIALIZED VIEW user_statistics AS
  WITH general AS (
    SELECT
      user_id,
      array_agg(id) changesets,
      max(coalesce(closed_at, created_at)) last_edit,
      count(*) changeset_count,
      sum(coalesce(total_edits, 0)) edit_count,
      max(updated_at) updated_at
    FROM changesets
    GROUP BY user_id
  ),
  country_counts AS (
    SELECT
      user_id,
      code,
      count(*) changesets,
      sum(coalesce(total_edits, 0)) edits
    FROM changesets
    JOIN changesets_countries ON changesets.id = changesets_countries.changeset_id
    JOIN countries ON changesets_countries.country_id = countries.id
    GROUP BY user_id, code
  ),
  countries AS (
    SELECT
      user_id,
      jsonb_object_agg(code, changesets) country_changesets,
      jsonb_object_agg(code, edits) country_edits
    FROM country_counts
    GROUP BY user_id
  ),
  edit_day_counts AS (
    SELECT
      user_id,
      date_trunc('day', coalesce(closed_at, created_at))::date AS day,
      count(*) changesets,
      sum(coalesce(total_edits, 0)) edits
    FROM changesets
    WHERE coalesce(closed_at, created_at) IS NOT NULL
    GROUP BY user_id, day
  ),
  edit_days AS (
    SELECT
      user_id,
      jsonb_object_agg(day, changesets) day_changesets,
      jsonb_object_agg(day, edits) day_edits
    FROM edit_day_counts
    GROUP BY user_id
  ),
  editor_counts AS (
    SELECT
      RANK() OVER (PARTITION BY user_id ORDER BY sum(coalesce(total_edits, 0)) DESC) AS rank,
      user_id,
      editor,
      count(*) changesets,
      sum(coalesce(total_edits, 0)) edits
    FROM changesets
    WHERE editor IS NOT NULL
    GROUP BY user_id, editor
  ),
  editors AS (
    SELECT
      user_id,
      jsonb_object_agg(editor, changesets) editor_changesets,
      jsonb_object_agg(editor, edits) editor_edits
    FROM editor_counts
    WHERE rank <= 10
    GROUP BY user_id
  ),
  hashtag_counts AS (
    SELECT
      RANK() OVER (PARTITION BY user_id ORDER BY sum(coalesce(total_edits, 0)) DESC) AS rank,
      user_id,
      hashtag,
      count(*) changesets,
      sum(coalesce(total_edits)) edits
    FROM changesets
    JOIN changesets_hashtags ON changesets.id = changesets_hashtags.changeset_id
    JOIN hashtags ON changesets_hashtags.hashtag_id = hashtags.id
    GROUP BY user_id, hashtag
  ),
  hashtags AS (
    SELECT
      user_id,
      jsonb_object_agg(hashtag, changesets) hashtag_changesets,
      jsonb_object_agg(hashtag, edits) hashtag_edits
    FROM hashtag_counts
    WHERE rank <= 50
    GROUP BY user_id
  ),
  measurements AS (
    SELECT
      id,
      user_id,
      key,
      value
    FROM changesets
    CROSS JOIN LATERAL jsonb_each(measurements)
  ),
  aggregated_measurements_kv AS (
    SELECT
      user_id,
      key,
      sum((value->>0)::numeric) AS value
    FROM measurements
    GROUP BY user_id, key
  ),
  aggregated_measurements AS (
    SELECT
      user_id,
      jsonb_object_agg(key, value) measurements
    FROM aggregated_measurements_kv
    GROUP BY user_id
  ),
  counts AS (
    SELECT
      id,
      user_id,
      key,
      value
    FROM changesets
    CROSS JOIN LATERAL jsonb_each(counts)
  ),
  aggregated_counts_kv AS (
    SELECT
      user_id,
      key,
      sum((value->>0)::numeric) AS value
    FROM counts
    GROUP BY user_id, key
  ),
  aggregated_counts AS (
    SELECT
      user_id,
      jsonb_object_agg(key, value) counts
    FROM aggregated_counts_kv
    GROUP BY user_id
  )
  SELECT
    user_id AS id,
    users.name,
    measurements,
    counts,
    last_edit,
    changeset_count,
    edit_count,
    editor_changesets,
    editor_edits,
    day_changesets,
    day_edits,
    country_changesets,
    country_edits,
    hashtag_changesets,
    hashtag_edits,
    updated_at
  FROM general
  LEFT OUTER JOIN countries USING (user_id)
  LEFT OUTER JOIN editors USING (user_id)
  LEFT OUTER JOIN edit_days USING (user_id)
  LEFT OUTER JOIN hashtags USING (user_id)
  LEFT OUTER JOIN aggregated_measurements USING (user_id)
  LEFT OUTER JOIN aggregated_counts USING (user_id)
  JOIN users ON user_id = users.id;

CREATE UNIQUE INDEX IF NOT EXISTS user_statistics_id ON user_statistics(id);
