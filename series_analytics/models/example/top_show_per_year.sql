WITH ranked_shows AS (
    SELECT
        title,
        rating,
        EXTRACT(YEAR FROM startDate) AS startYear,
        ROW_NUMBER() OVER(PARTITION BY EXTRACT(YEAR FROM startDate) ORDER BY rating DESC) as rank
    FROM {{ ref('anime') }}
    WHERE LOWER(type) = 'tv'
),

SELECT title, rating, startYear
FROM ranked_shows
ORDER BY year DESC
