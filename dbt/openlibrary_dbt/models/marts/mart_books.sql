{{ config(materialized='table') }}

WITH ranked_books AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY book_key
            ORDER BY extracted_at DESC, id DESC
        ) AS rn

    FROM {{ ref('stg_openlibrary_books') }}

),

latest_books AS (

    SELECT
        id,
        book_key,
        title,
        author_name,
        author_key,
        first_publish_year,
        language,
        edition_count,
        ebook_access,
        has_fulltext,
        cover_i,
        cover_edition_key,
        source,
        extracted_at

    FROM ranked_books
    WHERE rn = 1

)

SELECT
    id,
    book_key,
    title,
    author_name,
    author_key,

    first_publish_year,

    CASE
        WHEN first_publish_year IS NULL THEN 'Unknown'
        WHEN first_publish_year < 1950 THEN 'Before 1950'
        WHEN first_publish_year < 1980 THEN '1950-1979'
        WHEN first_publish_year < 2000 THEN '1980-1999'
        WHEN first_publish_year < 2010 THEN '2000-2009'
        WHEN first_publish_year < 2020 THEN '2010-2019'
        ELSE '2020+'
    END AS publication_period,

    CASE
        WHEN first_publish_year IS NULL THEN NULL
        ELSE FLOOR(first_publish_year / 10.0) * 10
    END AS publication_decade,

    language,

    edition_count,

    CASE
        WHEN edition_count IS NULL THEN 'Unknown'
        WHEN edition_count = 1 THEN '1 edition'
        WHEN edition_count BETWEEN 2 AND 5 THEN '2-5 editions'
        WHEN edition_count BETWEEN 6 AND 10 THEN '6-10 editions'
        ELSE 'More than 10 editions'
    END AS edition_group,

    ebook_access,

    CASE
        WHEN ebook_access IN ('borrowable', 'public') THEN TRUE
        ELSE FALSE
    END AS ebook_available,

    has_fulltext,

    CASE
        WHEN has_fulltext = TRUE THEN 'Available'
        ELSE 'Not Available'
    END AS fulltext_status,

    cover_i,
    cover_edition_key,

    source,
    extracted_at

FROM latest_books