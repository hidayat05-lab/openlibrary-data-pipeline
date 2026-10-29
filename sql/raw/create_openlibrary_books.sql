CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.openlibrary_books (
    id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    source TEXT NOT NULL,
    extracted_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL
);