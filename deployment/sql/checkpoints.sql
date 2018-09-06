CREATE TABLE checkpoints (
    proc_name text NOT NULL UNIQUE,
    sequence integer NOT NULL,
    PRIMARY KEY(proc_name)
);

