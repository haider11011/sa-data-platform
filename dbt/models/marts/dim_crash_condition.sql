-- Junk dimension: the low-cardinality descriptive flags of a crash, bundled
-- into one dimension instead of five near-empty ones or five text columns
-- repeated across 63k fact rows. The surrogate key is a deterministic hash of
-- the attribute values, so the fact table computes the identical key from its
-- own columns — no join needed at build time, and the combination stays
-- stable across rebuilds.

with combinations as (

    select distinct
        crash_type,
        weather_condition,
        moisture_condition,
        day_night,
        is_dui_involved,
        is_drugs_involved
    from {{ ref('stg_data_sa__crashes') }}

)

select
    {{ dbt_utils.generate_surrogate_key([
        'crash_type', 'weather_condition', 'moisture_condition',
        'day_night', 'is_dui_involved', 'is_drugs_involved'
    ]) }} as condition_key,
    crash_type,
    weather_condition,
    moisture_condition,
    day_night,
    is_dui_involved,
    is_drugs_involved
from combinations
