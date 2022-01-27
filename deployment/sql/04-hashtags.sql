CREATE TABLE hashtags (
    id serial,
    hashtag text NOT NULL UNIQUE,
    PRIMARY KEY(id)
);

CREATE UNIQUE INDEX ON hashtags (hashtag);

-- support for LIKE queries on hashtags
CREATE EXTENSION pg_trgm;
CREATE INDEX trgm_idx_hashtags ON hashtags USING gin (hashtag gin_trgm_ops);
