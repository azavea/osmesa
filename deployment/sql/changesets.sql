CREATE TABLE changesets (
    id bigint NOT NULL,
    measurements jsonb,
    counts jsonb,
    total_edits integer,
    editor text,
    user_id integer,
    created_at timestamp with time zone,
    closed_at timestamp with time zone,
    augmented_diffs integer[],
    updated_at timestamp with time zone,
    PRIMARY KEY(id)
);

CREATE INDEX changesets_user_id ON changesets(user_id);

CREATE INDEX changesets_created_at_index
    ON changesets (created_at);

CREATE INDEX changesets_closed_at_index
    ON changesets (closed_at);

CREATE INDEX changesets_updated_at_index
    ON changesets (updated_at);

