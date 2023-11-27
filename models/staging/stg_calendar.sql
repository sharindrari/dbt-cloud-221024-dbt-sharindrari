{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns',
    unique_key = 'id'
  )
}}

SELECT {{ surrogate_key(['date', 'listing_id']) }} AS id, --the granularity of this base table is date, listing_id
    date,
    listing_id,
    available AS is_available,
    reservation_id,
    price AS price_on_date,
    maximum_nights AS max_nights_per_stay,
    minimum_nights AS min_nights_per_stay
FROM `test-db-405913.test_raw.calendar`

{% if is_incremental() %}

  -- (uses > to include records whose timestamp occurred since the latest date of this model)
  where date > (select max(date) from {{ this }})

{% endif %}
