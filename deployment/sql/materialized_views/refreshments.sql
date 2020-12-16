CREATE TABLE refreshments (
  mat_view text NOT NULL,
  updated_at timestamp with time zone,
  PRIMARY KEY(mat_view)
);

INSERT INTO refreshments VALUES ('user_statistics', to_timestamp(0)), ('country_statistics', to_timestamp(0)), ('hashtag_statistics', to_timestamp(0)), ('hashtag_user_statistics', to_timestamp(0));