{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns',
  )
}}

SELECT listing_id,
    DATE(change_at) AS change_date,
    amenities
FROM `test-db-405913.test_raw.amenities_changelog`

{% if is_incremental() %}

  -- (uses > to include records whose timestamp occurred since the latest date of this model)
  where change_at > (select max(change_at) from {{ this }})

{% endif %}