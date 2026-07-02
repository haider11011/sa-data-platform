-- Silver-layer view over raw casualty records (one row per person injured).
-- Same REPORT_ID normalisation as the crash staging model so the two join.

with source as (

    select * from {{ source('data_sa', 'data_sa_casualties') }}

),

renamed as (

    select
        regexp_replace("REPORT_ID", '-\d{1,2}/\d{1,2}/\d{4}$', '')  as report_id,
        "UND_UNIT_NUMBER"::int                                      as unit_number,
        "CASUALTY_NUMBER"::int                                      as casualty_number,

        "Casualty Type"                                             as casualty_type,
        "Sex"                                                       as sex,
        -- ages come zero-padded ('041') with 'XX'/'XXX' for masked/unknown
        case when "AGE" ~ '^\d+$' then "AGE"::int end               as age,

        "Position In Veh"                                           as position_in_vehicle,
        "Thrown Out"                                                as thrown_out,
        "Injury Extent"                                             as injury_extent,
        "Seat Belt"                                                 as seat_belt,
        "Helmet"                                                    as helmet,
        -- hospital names are masked as 'XXXXXX' in some rows: real NULL is honest
        nullif("Hospital", 'XXXXXX')                                as hospital,

        _source_resource,
        _ingested_at

    from source

)

select * from renamed
