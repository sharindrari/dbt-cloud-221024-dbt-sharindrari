WITH base AS (
SELECT 
       id,
       review_date,
       listing_id,
       review_score,
       AVG(review_score) OVER (PARTITION BY listing_id ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS current_avg_rating
FROM {{ ref( "stg_reviews" ) }}
ORDER BY 1,2,3
),
last_avg_review_daily AS (
SELECT *,
    COALESCE(DATE_SUB(LEAD(review_date) OVER (PARTITION BY listing_id ORDER BY id), INTERVAL 1 DAY), CURRENT_DATE) AS EFFECTIVE_TO_DATE,
    LAST_VALUE(current_avg_rating) OVER (PARTITION BY review_date, listing_id ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_average_on_review_date
FROM base
ORDER BY LISTING_ID, REVIEW_DATE 
)
SELECT review_date, 
    listing_id, 
    COUNT(*) AS num_of_reviews,
    MAX(effective_to_date) AS effective_to_date,
    MAX(last_average_on_review_date) AS current_avg_rating
FROM last_avg_review_daily
GROUP BY 1,2