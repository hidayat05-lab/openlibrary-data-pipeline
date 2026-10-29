{{ config(materialized='view') }}

WITH source AS (

    SELECT
        id,
        batch_id,
        source,
        extracted_at,
        payload
    FROM {{ source('raw', 'openlibrary_books') }}

),

staged AS (

    SELECT
        id,
        batch_id,
        source,
        extracted_at,

        payload->>'key' AS book_key,
        payload->>'title' AS title,

        payload->'author_name' AS author_name,
        payload->'author_key' AS author_key,

        NULLIF(payload->>'first_publish_year', '')::INTEGER
            AS first_publish_year,

        payload->'language' AS language,

        NULLIF(payload->>'edition_count', '')::INTEGER
            AS edition_count,

        payload->>'ebook_access' AS ebook_access,

        COALESCE(
            (payload->>'has_fulltext')::BOOLEAN,
            FALSE
        ) AS has_fulltext,

        NULLIF(payload->>'cover_i', '')::INTEGER
            AS cover_i,

        payload->>'cover_edition_key' AS cover_edition_key

    FROM source

)

SELECT *
FROM staged