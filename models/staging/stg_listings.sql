{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns',
    unique_key = 'id'
  )
}}

SELECT id,
    name,
    host_id,
    host_name,
    host_since,
    host_location,
    host_verifications, 
    neighborhood,
    property_type,
    room_type,
    accommodates AS max_num_of_guests,
    bathrooms_text AS num_and_types_of_bathroom,
    beds AS num_of_beds,
    number_of_reviews,
    first_review AS first_review_date,
    last_review AS last_review_date,
    review_scores_rating AS avg_review_score
FROM `test-db-405913.test_raw.listings`