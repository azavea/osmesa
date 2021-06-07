CREATE TABLE errors (
    id bigint NOT NULL,
    type smallint,
    sequence integer,
    tags jsonb,
    nds bigint[],
    changeset bigint,
    uid bigint,
    "user" text,
    updated timestamp with time zone,
    visible boolean,
    version integer,
    PRIMARY KEY(id)
);
