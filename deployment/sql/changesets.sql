CREATE TABLE changesets (
    id bigint NOT NULL,
    measurements jsonb,
    counts jsonb,
    editor text,
    user_id integer,
    created_at timestamp with time zone,
    closed_at timestamp with time zone,
    augmented_diffs integer[],
    updated_at timestamp with time zone,
    PRIMARY KEY(id)
);

CREATE INDEX changesets_user_id ON changesets(user_id);
