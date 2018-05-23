CREATE TABLE hashtags (
    id serial,
    hashtag text NOT NULL UNIQUE,
    PRIMARY KEY(id)
);

CREATE UNIQUE INDEX ON hashtags (hashtag);
