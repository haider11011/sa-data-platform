-- Silver-layer view over raw crash records: renaming, typing and light
-- normalisation only. Business logic (region conformance, dimensional keys)
-- deliberately lives downstream so this model stays a faithful, typed mirror
-- of the source.

with source as (

    select * from {{ source('data_sa', 'data_sa_crashes') }}

),

renamed as (

    select
        -- The source REPORT_ID embeds the extract date ("2020-1-19/09/2025").
        -- We strip it to the stable year-sequence key ("2020-1"): the suffix
        -- changes with every extract, so keeping it would break comparisons
        -- between warehouse builds ingested from different extracts.
        regexp_replace("REPORT_ID", '-\d{1,2}/\d{1,2}/\d{4}$', '')  as report_id,

        -- geography (free-text from the source; conformed to ABS codes downstream)
        "Stats Area"                                                as stats_area,
        initcap("Suburb")                                           as suburb,
        "Postcode"                                                  as postcode,
        "LGA Name"                                                  as lga_name,

        -- outcome measures
        "Total Units"::int                                          as total_units,
        "Total Cas"::int                                            as total_casualties,
        "Total Fats"::int                                           as total_fatalities,
        "Total SI"::int                                             as total_serious_injuries,
        "Total MI"::int                                             as total_minor_injuries,

        -- when
        to_timestamp("Crash Date Time", 'DD/MM/YYYY HH24:MI:SS')    as crash_at,
        to_timestamp("Crash Date Time", 'DD/MM/YYYY HH24:MI:SS')::date as crash_date,
        "Year"::int                                                 as crash_year,

        -- where / conditions
        "Area Speed"::int                                           as area_speed_limit,
        "Position Type"                                             as position_type,
        "Horizontal Align"                                          as horizontal_alignment,
        "Vertical Align"                                            as vertical_alignment,
        "Road Surface"                                              as road_surface,
        "Moisture Cond"                                             as moisture_condition,
        "Weather Cond"                                              as weather_condition,
        "DayNight"                                                  as day_night,

        -- classification
        "Crash Type"                                                as crash_type,
        -- "CSEF Severity" packs code and label into one field ("4: Fatal");
        -- split so facts can carry the compact code and dims the readable label
        split_part("CSEF Severity", ':', 1)::int                    as severity_code,
        trim(split_part("CSEF Severity", ':', 2))                   as severity_label,
        "Traffic Ctrls"                                             as traffic_controls,

        -- source uses 'Y' / NULL; NULL means "not recorded as involved", which
        -- we treat as false — documented assumption, there is no explicit 'N'
        coalesce("DUI Involved" = 'Y', false)                       as is_dui_involved,
        coalesce("Drugs Involved" = 'Y', false)                     as is_drugs_involved,

        -- location (GDA94 / SA Lambert projected coordinates, as published)
        "ACCLOC_X"::numeric                                         as location_x,
        "ACCLOC_Y"::numeric                                         as location_y,
        "UNIQUE_LOC"                                                as location_key,

        _source_resource,
        _ingested_at

    from source

)

select * from renamed
