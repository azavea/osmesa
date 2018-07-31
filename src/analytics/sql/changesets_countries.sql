CREATE TABLE changesets_countries (
    changeset_id integer NOT NULL,
    country_id integer NOT NULL,
    edit_count integer NOT NULL,
    PRIMARY KEY(changeset_id, country_id)
);
