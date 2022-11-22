SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;;

SET search_path = public, pg_catalog;
SET default_tablespace = '';
SET default_with_oids = false;

/* User profile */
CREATE TABLE IF NOT EXISTS user_profile (
    id BIGINT NOT NULL,
    ingested_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

/* Content profile */
CREATE TABLE IF NOT EXISTS content_profile (
    id BIGINT NOT NULL,
    title CHARACTER VARYING NULL,
    genres CHARACTER VARYING [] NULL,
    tags JSONB NULL,
    tmdb_id BIGINT NULL,
    tmdb_primary_info JSONB NULL,
    tmdb_credits JSON NULL,
    tmdb_keywords CHARACTER VARYING [] NULL,
    imdb_id BIGINT NULL,
    imdb_primary_info JSONB NULL,
    ingested_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

/* User rating */
CREATE TABLE IF NOT EXISTS user_rating (
    user_id BIGINT NOT NULL,
    content_id BIGINT NOT NULL,
    rated_at TIMESTAMP WITHOUT TIME ZONE NULL,
    rating FLOAT NOT NULL DEFAULT 0.0,
    ingested_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES user_profile (id) ON DELETE CASCADE,
    FOREIGN KEY (content_id) REFERENCES content_profile (id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, content_id, rated_at)
);

/* User tagging */
CREATE TABLE IF NOT EXISTS user_tagging (
    user_id BIGINT NOT NULL,
    content_id BIGINT NOT NULL,
    tag CHARACTER VARYING NOT NULL,
    relevance FLOAT NULL,
    tagged_at TIMESTAMP WITHOUT TIME ZONE NULL,
    ingested_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES user_profile (id) ON DELETE CASCADE,
    FOREIGN KEY (content_id) REFERENCES content_profile (id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, content_id, tag)
);
