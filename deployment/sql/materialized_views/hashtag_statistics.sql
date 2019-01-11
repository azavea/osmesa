WITH hashtag_join AS (
        SELECT chg.id,
           chg.road_km_added,
           chg.road_km_modified,
           chg.waterway_km_added,
           chg.waterway_km_modified,
           chg.coastline_km_added,
           chg.coastline_km_modified,
           chg.roads_added,
           chg.roads_modified,
           chg.waterways_added,
           chg.waterways_modified,
           chg.coastline_added,
           chg.coastline_modified,
           chg.buildings_added,
           chg.buildings_modified,
           chg.pois_added,
           chg.pois_modified,
           chg.editor,
           chg.user_id,
           chg.created_at,
           chg.closed_at,
           chg.augmented_diffs,
           chg.updated_at,
           ch.hashtag_id
          FROM (changesets chg
            JOIN changesets_hashtags ch ON ((ch.changeset_id = chg.id)))
       ), hashtag_usr_counts AS (
        SELECT hashtag_join.hashtag_id,
           users.id AS uid,
           array_agg(DISTINCT users.name) AS names,
           count(*) AS edit_count
          FROM (users
            JOIN hashtag_join ON ((hashtag_join.user_id = users.id)))
         GROUP BY hashtag_join.hashtag_id, users.id
       ), usr_json_agg AS (
        SELECT hashtag_usr_counts.hashtag_id,
           json_agg(json_build_object('name', hashtag_usr_counts.names[1], 'uid', hashtag_usr_counts.uid, 'edits', hashtag_usr_counts.edit_count)) AS users
          FROM hashtag_usr_counts
         GROUP BY hashtag_usr_counts.hashtag_id
       ), without_json AS (
        SELECT ht.hashtag AS tag,
           ht.id AS hashtag_id,
           (('hashtag/'::text || ht.hashtag) || '/{z}/{x}/{y}.mvt'::text) AS extent_uri,
           sum(hashtag_join.buildings_added) AS buildings_added,
           sum(hashtag_join.buildings_modified) AS buildings_modified,
           sum(hashtag_join.roads_added) AS roads_added,
           sum(hashtag_join.road_km_added) AS road_km_added,
           sum(hashtag_join.roads_modified) AS roads_modified,
           sum(hashtag_join.road_km_modified) AS road_km_modified,
           sum(hashtag_join.waterways_added) AS waterways_added,
           sum(hashtag_join.waterway_km_added) AS waterway_km_added,
           sum(hashtag_join.waterways_modified) AS waterways_modified,
           sum(hashtag_join.waterway_km_modified) AS waterway_km_modified,
           sum(hashtag_join.coastline_added) AS coastline_added,
           sum(hashtag_join.coastline_km_added) AS coastline_km_added,
           sum(hashtag_join.coastline_modified) AS coastline_modified,
           sum(hashtag_join.coastline_km_modified) AS coastline_km_modified,
           sum(hashtag_join.pois_added) AS pois_added,
           sum(hashtag_join.pois_modified) AS pois_modified
          FROM (hashtags ht
            JOIN hashtag_join ON ((ht.id = hashtag_join.hashtag_id)))
         GROUP BY ht.id, ht.hashtag
       )
  SELECT without_json.tag,
     without_json.hashtag_id,
     without_json.extent_uri,
     without_json.buildings_added,
     without_json.buildings_modified,
     without_json.roads_added,
     without_json.road_km_added,
     without_json.roads_modified,
     without_json.road_km_modified,
     without_json.waterways_added,
     without_json.waterway_km_added,
     without_json.waterways_modified,
     without_json.waterway_km_modified,
     without_json.coastline_added,
     without_json.coastline_km_added,
     without_json.coastline_modified,
     without_json.coastline_km_modified,
     without_json.pois_added,
     without_json.pois_modified,
     usr_json_agg.users
    FROM (without_json
      JOIN usr_json_agg ON ((without_json.hashtag_id = usr_json_agg.hashtag_id)));
