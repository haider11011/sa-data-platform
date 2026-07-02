-- Silver-layer view over raw ABS ERP observations, filtered to South
-- Australian LGAs. Raw holds every Australian LGA (faithful to the source);
-- the SA filter is business scope, so it belongs here, not in ingestion.

with source as (

    select * from {{ source('abs', 'abs_erp_lga') }}

),

renamed as (

    select
        "LGA_2024"                                      as lga_code,

        case "SEX_ABS"
            when '1' then 'Male'
            when '2' then 'Female'
            when '3' then 'Persons'
        end                                             as sex,

        "AGE"                                           as age_band_code,

        "TIME_PERIOD"::int                              as year,
        "OBS_VALUE"::int                                as population,

        _source_resource,
        _ingested_at

    from source
    where "REGION_TYPE" = 'LGA2024'
      -- ABS LGA codes prefix the state code; 4 = South Australia.
      -- The bare code '4' is the state total, which would double-count.
      and "LGA_2024" like '4%'
      and "LGA_2024" <> '4'

)

select * from renamed
