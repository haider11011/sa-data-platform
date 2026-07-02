-- Fact table. Grain: ONE ROW PER REPORTED CRASH (2020-2024 extract).
--
-- Foreign keys: dim_region (SCD2 version current at crash date), dim_date
-- (crash_date), dim_crash_condition (hash computed from the crash's own
-- attributes — see that model for why). report_id is a degenerate dimension:
-- it identifies the transaction but has no attributes of its own.
--
-- Measures are additive counts as published by the source. stats_area,
-- suburb, postcode and speed limit stay on the fact: they describe the crash
-- site itself, and 7 of 71 LGAs span multiple stats areas, so stats_area is
-- provably not a region-level attribute.

with crashes as (

    select * from {{ ref('stg_data_sa__crashes') }}

),

lga_mapping as (

    select * from {{ ref('int_lga_name_conformed') }}

),

regions as (

    select * from {{ ref('dim_region') }}

)

select
    crashes.report_id,

    regions.region_key,
    crashes.crash_date,
    {{ dbt_utils.generate_surrogate_key([
        'crashes.crash_type', 'crashes.weather_condition', 'crashes.moisture_condition',
        'crashes.day_night', 'crashes.is_dui_involved', 'crashes.is_drugs_involved'
    ]) }} as condition_key,

    -- degenerate / site attributes
    crashes.crash_at,
    crashes.severity_code,
    crashes.severity_label,
    crashes.stats_area,
    crashes.suburb,
    crashes.postcode,
    crashes.area_speed_limit,
    crashes.crash_type,

    -- measures
    crashes.total_units,
    crashes.total_casualties,
    crashes.total_fatalities,
    crashes.total_serious_injuries,
    crashes.total_minor_injuries

from crashes
left join lga_mapping
    on lga_mapping.source_lga_name = crashes.lga_name
join regions
    -- unmatched or missing LGA names route to the explicit Unknown member
    -- rather than dropping fact rows or leaving a NULL foreign key
    on regions.lga_code = coalesce(lga_mapping.lga_code, 'UNK')
    and crashes.crash_date >= regions.valid_from
    and crashes.crash_date < regions.valid_to
