WITH country_counts AS (
        SELECT cc.changeset_id,
           countries.name,
           cc.edit_count
          FROM (changesets_countries cc
            JOIN countries ON ((cc.country_id = countries.id)))
       ), chgset_country_counts AS (
        SELECT chg.user_id,
           country_counts.name,
           sum(country_counts.edit_count) AS edit_count
          FROM (country_counts
            JOIN changesets chg ON ((country_counts.changeset_id = chg.id)))
         GROUP BY chg.user_id, country_counts.name
       ), usr_country_counts AS (
        SELECT chgset_country_counts.user_id,
           json_agg(json_build_object('name', chgset_country_counts.name, 'count', chgset_country_counts.edit_count)) AS country_json
          FROM chgset_country_counts
         GROUP BY chgset_country_counts.user_id
       ), day_counts AS (
        SELECT chg.user_id,
           to_char(date_trunc('day'::text, chg.created_at), 'YYYY-MM-DD'::text) AS day,
           count(*) AS cnt
          FROM changesets chg
         WHERE (chg.created_at IS NOT NULL)
         GROUP BY chg.user_id, (date_trunc('day'::text, chg.created_at))
       ), usr_day_counts AS (
        SELECT day_counts.user_id,
           json_agg(json_build_object('day', day_counts.day, 'count', day_counts.cnt)) AS day_json
          FROM day_counts
         GROUP BY day_counts.user_id
       ), editor_counts AS (
        SELECT chg.user_id,
           chg.editor,
           count(*) AS cnt
          FROM changesets chg
         WHERE (chg.editor IS NOT NULL)
         GROUP BY chg.user_id, chg.editor
       ), usr_editor_counts AS (
        SELECT editor_counts.user_id,
           json_agg(json_build_object('editor', editor_counts.editor, 'count', editor_counts.cnt)) AS editor_json
          FROM editor_counts
         GROUP BY editor_counts.user_id
       ), hashtag_counts AS (
        SELECT ch.changeset_id,
           hashtags.hashtag,
           count(*) AS edit_count
          FROM (changesets_hashtags ch
            JOIN hashtags ON ((ch.hashtag_id = hashtags.id)))
         GROUP BY ch.changeset_id, hashtags.hashtag
       ), chgset_ht_counts AS (
        SELECT chg.user_id,
            hashtag_counts.hashtag,
            count(*) AS cnt
            FROM (changesets chg
            JOIN hashtag_counts ON ((chg.id = hashtag_counts.changeset_id)))
            GROUP BY chg.user_id, hashtag_counts.hashtag
       ), usr_hashtag_counts AS (
        SELECT chgset_ht_counts.user_id,
           json_agg(json_build_object('tag', chgset_ht_counts.hashtag, 'count', chgset_ht_counts.cnt)) AS hashtag_json
          FROM chgset_ht_counts
         GROUP BY chgset_ht_counts.user_id
       ), agg_stats AS (
        SELECT usr.id,
           usr.name,
           (('user/'::text || usr.name) || '/{z}/{x}/{y}.mvt'::text) AS extent_uri,
           array_agg(chg.id) AS changesets,
           sum(chg.road_km_added) AS road_km_added,
           sum(chg.road_km_modified) AS road_km_modified,
           sum(chg.waterway_km_added) AS waterway_km_added,
           sum(chg.waterway_km_modified) AS waterway_km_modified,
           sum(chg.roads_added) AS roads_added,
           sum(chg.roads_modified) AS roads_modified,
           sum(chg.waterways_added) AS waterways_added,
           sum(chg.waterways_modified) AS waterways_modified,
           sum(chg.buildings_added) AS buildings_added,
           sum(chg.buildings_modified) AS buildings_modified,
           sum(chg.pois_added) AS pois_added,
           sum(chg.pois_modified) AS pois_modified,
           count(*) AS changeset_count,
           count(*) AS edit_count,
           max(COALESCE(chg.closed_at, chg.created_at, chg.updated_at)) AS updated_at
          FROM (changesets chg
            JOIN users usr ON ((chg.user_id = usr.id)))
         WHERE (chg.user_id IS NOT NULL)
         GROUP BY usr.id, usr.name
       )
  SELECT agg_stats.id,
     agg_stats.name,
     agg_stats.extent_uri,
     agg_stats.changesets,
     agg_stats.road_km_added,
     agg_stats.road_km_modified,
     agg_stats.waterway_km_added,
     agg_stats.waterway_km_modified,
     agg_stats.roads_added,
     agg_stats.roads_modified,
     agg_stats.waterways_added,
     agg_stats.waterways_modified,
     agg_stats.buildings_added,
     agg_stats.buildings_modified,
     agg_stats.pois_added,
     agg_stats.pois_modified,
     agg_stats.changeset_count,
     agg_stats.edit_count,
     usr_editor_counts.editor_json AS editors,
     usr_day_counts.day_json AS edit_times,
     usr_country_counts.country_json AS country_list,
     coalesce(usr_hashtag_counts.hashtag_json, '[]') AS hashtags,
     agg_stats.updated_at
    FROM ((((agg_stats
      LEFT JOIN usr_country_counts ON ((agg_stats.id = usr_country_counts.user_id)))
      LEFT JOIN usr_hashtag_counts ON ((agg_stats.id = usr_hashtag_counts.user_id)))
      LEFT JOIN usr_day_counts ON ((agg_stats.id = usr_day_counts.user_id)))
      LEFT JOIN usr_editor_counts ON ((agg_stats.id = usr_editor_counts.user_id)));

