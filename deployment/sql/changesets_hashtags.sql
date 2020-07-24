CREATE TABLE changesets_hashtags (
    changeset_id integer NOT NULL
        CONSTRAINT changesets_hashtags_changesets_id_fk
        REFERENCES changesets,
    hashtag_id integer NOT NULL
        CONSTRAINT changesets_hashtags_hashtags_id_fk
        REFERENCES hashtags,
    PRIMARY KEY(changeset_id, hashtag_id)
);

-- support joining on foreign keys (add index in reverse order of the primary key)
CREATE INDEX changesets_hashtags_hashtag_id_changeset_id_index
    ON changesets_hashtags (hashtag_id, changeset_id);
