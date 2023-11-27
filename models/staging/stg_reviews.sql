{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns',
    unique_key = 'id'
  )
}}

SELECT id,
    listing_id,
    review_score,
    review_date
FROM `test-db-405913.test_raw.generated_reviews`