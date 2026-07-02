-- Fact table. Grain: ONE ROW PER PERSON INJURED in a crash (a casualty).
--
-- Region and date are inherited from the parent crash — casualties happen
-- where and when their crash happens — so this fact shares dim_region and
-- dim_date with fact_road_crashes and fact_population_annual. Exact casualty
-- ages are bucketed into the ABS age bands (dim_age_band), which is what
-- makes casualty rates per age group divisible by the matching population.

with casualties as (

    select * from {{ ref('stg_data_sa__casualties') }}

),

crashes as (

    select * from {{ ref('stg_data_sa__crashes') }}

),

lga_mapping as (

    select * from {{ ref('int_lga_name_conformed') }}

),

regions as (

    select * from {{ ref('dim_region') }}

),

age_bands as (

    select * from {{ ref('dim_age_band') }}

)

select
    -- composite business key of the casualty within its crash
    casualties.report_id,
    casualties.unit_number,
    casualties.casualty_number,

    regions.region_key,
    crashes.crash_date,
    age_bands.age_band_code,   -- null when the source masked the age (XX/XXX)

    casualties.sex,
    casualties.age,
    casualties.casualty_type,
    casualties.injury_extent,
    casualties.position_in_vehicle,
    casualties.thrown_out,
    casualties.seat_belt,
    casualties.helmet,
    casualties.hospital,
    crashes.severity_code as crash_severity_code,

    1 as casualty_count

from casualties
join crashes
    on crashes.report_id = casualties.report_id
left join lga_mapping
    on lga_mapping.source_lga_name = crashes.lga_name
join regions
    on regions.lga_code = coalesce(lga_mapping.lga_code, 'UNK')
    and crashes.crash_date >= regions.valid_from
    and crashes.crash_date < regions.valid_to
left join age_bands
    on casualties.age between age_bands.min_age and age_bands.max_age
