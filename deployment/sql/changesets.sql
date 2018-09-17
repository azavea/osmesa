CREATE TABLE changesets (
    id bigint NOT NULL,
    road_km_added double precision NOT NULL DEFAULT 0.0,
    road_km_modified double precision NOT NULL DEFAULT 0.0,
    waterway_km_added double precision NOT NULL DEFAULT 0.0,
    waterway_km_modified double precision NOT NULL DEFAULT 0.0,
    roads_added integer NOT NULL DEFAULT 0,
    roads_modified integer NOT NULL DEFAULT 0,
    waterways_added integer NOT NULL DEFAULT 0,
    waterways_modified integer NOT NULL DEFAULT 0,
    buildings_added integer NOT NULL DEFAULT 0,
    buildings_modified integer NOT NULL DEFAULT 0,
    pois_added integer NOT NULL DEFAULT 0,
    pois_modified integer NOT NULL DEFAULT 0,
    editor text,
    user_id integer,
    created_at timestamp with time zone,
    closed_at timestamp with time zone,
    augmented_diffs integer[],
    updated_at timestamp with time zone,
    PRIMARY KEY(id)
);

CREATE INDEX changesets_user_id ON changesets(user_id);
