CREATE TABLE changesets_countries (
    changeset_id integer NOT NULL
        CONSTRAINT changesets_countries_changesets_id_fk
		REFERENCES changesets,
    country_id integer NOT NULL
        CONSTRAINT changesets_countries_countries_id_fk
		REFERENCES countries,
    edit_count integer NOT NULL,
    augmented_diffs integer[],
    PRIMARY KEY(changeset_id, country_id)
);

-- support joining on foreign keys (add index in reverse order of the primary key)
CREATE INDEX changesets_countries_country_id_changeset_id_index
	ON changesets_countries (country_id, changeset_id);
