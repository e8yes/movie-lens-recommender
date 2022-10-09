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

/* IMDB */
CREATE TABLE IF NOT EXISTS imdb (
    id BIGINT NOT NULL,
    primary_info JSONB NULL,
    ingested_at TIMESTAMP WITHOUT TIME ZONE NULL,
    PRIMARY KEY (id)
);

/* TMDB */
CREATE TABLE IF NOT EXISTS tmdb (
    id BIGINT NOT NULL,
    primary_info JSONB NULL,
    credits JSONB NULL,
    ingested_at TIMESTAMP WITHOUT TIME ZONE NULL,
    PRIMARY KEY (id)
);

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
    genome_scores JSONB Null,
    tags JSONB NULL,
    imdb_id BIGINT NULL,
    tmdb_id BIGINT NULL,
    ingested_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (imdb_id) REFERENCES imdb (id) ON DELETE SET NULL,
    FOREIGN KEY (tmdb_id) REFERENCES tmdb (id) ON DELETE SET NULL,
    PRIMARY KEY (id)
);

/* User feedback */
CREATE TABLE IF NOT EXISTS user_feedback (
    user_id BIGINT NOT NULL,
    content_id BIGINT NOT NULL,
    rating FLOAT NOT NULL,
    rated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    ingested_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES user_profile (id) ON DELETE CASCADE,
    FOREIGN KEY (content_id) REFERENCES content_profile (id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, content_id)
);
