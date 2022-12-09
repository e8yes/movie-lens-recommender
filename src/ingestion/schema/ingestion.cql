USE ingestion;

/* User profile */
CREATE TABLE IF NOT EXISTS user_profile (
    id BIGINT,
    ingested_at TIMESTAMP,
    PRIMARY KEY (id)
);

/* Content profile */
CREATE TABLE IF NOT EXISTS content_profile (
    id BIGINT,
    title VARCHAR,
    genres LIST<VARCHAR>,
    tags VARCHAR,
    tmdb_id BIGINT,
    tmdb_primary_info VARCHAR,
    tmdb_credits VARCHAR,
    tmdb_keywords VARCHAR,
    imdb_id BIGINT,
    imdb_primary_info VARCHAR,
    ingested_at TIMESTAMP,
    PRIMARY KEY (id)
);

/* User rating */
CREATE TABLE IF NOT EXISTS user_rating (
    user_id BIGINT,
    content_id BIGINT,
    rated_at TIMESTAMP,
    rating FLOAT,
    ingested_at TIMESTAMP,
    PRIMARY KEY (user_id, content_id, rated_at)
);

/* User tagging */
CREATE TABLE IF NOT EXISTS user_tagging (
    user_id BIGINT,
    content_id BIGINT,
    tag VARCHAR,
    relevance FLOAT,
    tagged_at TIMESTAMP,
    ingested_at TIMESTAMP,
    PRIMARY KEY (user_id, content_id, tag)
);
