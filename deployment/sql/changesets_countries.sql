CREATE TABLE changesets_countries (
    changeset_id integer NOT NULL,
    country_id integer NOT NULL,
    edit_count integer NOT NULL,
    augmented_diffs integer[],
    PRIMARY KEY(changeset_id, country_id)
);
