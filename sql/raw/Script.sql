select 
count(*) as TOTAL_BOOKS,
count(distinct book_key) as unik_book,
count(*) filter (where book_key is null) as key_null_book
from analytics.mart_books;

SELECT
    COUNT(*) AS total_books,
    COUNT(*) FILTER (
        WHERE title IS NULL OR TRIM(title) = ''
    ) AS missing_title,
    COUNT(*) FILTER (
        WHERE first_publish_year IS NULL
    ) AS missing_year,
    COUNT(*) FILTER (
        WHERE edition_count IS NULL
    ) AS missing_edition_count
FROM analytics.mart_books;


select * from analytics.mart_books
limit 20;

SELECT
    publication_decade,
    COUNT(*) AS total_books
FROM analytics.mart_books
WHERE publication_decade IS NOT NULL
GROUP BY publication_decade
ORDER BY total_books DESC
LIMIT 15;


SELECT
    ebook_available,
    COUNT(*) AS total_books,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    ) AS percentage
FROM analytics.mart_books
GROUP BY ebook_available
ORDER BY total_books DESC;


SELECT
    fulltext_status,
    COUNT(*) AS total_books,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    ) AS percentage
FROM analytics.mart_books
GROUP BY fulltext_status
ORDER BY total_books DESC;

SELECT
    book_key,
    title,
    first_publish_year,
    edition_count
FROM analytics.mart_books
WHERE edition_count IS NOT NULL
ORDER BY edition_count DESC
LIMIT 10;

SELECT
    publication_period,
    COUNT(*) AS total_books,
    ROUND(AVG(edition_count), 2) AS avg_edition_count
FROM analytics.mart_books
GROUP BY publ ication_period
ORDER BY total_books DESC;