CREATE TABLE changesets_hashtags (
    changeset_id integer NOT NULL,
    hashtag_id integer NOT NULL,
    PRIMARY KEY(changeset_id, hashtag_id)
);
