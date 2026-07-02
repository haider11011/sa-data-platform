-- Silver-layer view over raw unit records (one row per vehicle, pedestrian or
-- cyclist involved in a crash). Not yet modelled into the star schema; staged
-- so the warehouse exposes the full extract in a clean, typed form.

with source as (

    select * from {{ source('data_sa', 'data_sa_units') }}

),

renamed as (

    select
        regexp_replace("REPORT_ID", '-\d{1,2}/\d{1,2}/\d{4}$', '')  as report_id,
        "Unit No"::int                                              as unit_number,

        "No Of Cas"::int                                            as casualty_count,
        "Unit Type"                                                 as unit_type,
        "Veh Reg State"                                             as vehicle_registration_state,
        case when "Veh Year" ~ '^\d{4}$' then "Veh Year"::int end   as vehicle_year,
        "Direction Of Travel"                                       as direction_of_travel,
        "Sex"                                                       as driver_sex,
        case when "Age" ~ '^\d+$' then "Age"::int end               as driver_age,
        "Lic State"                                                 as licence_state,
        "Licence Class"                                             as licence_class,
        "Licence Type"                                              as licence_type,
        "Towing"                                                    as towing,
        "Unit Movement"                                             as unit_movement,
        case when "Number Occupants" ~ '^\d+$'
             then "Number Occupants"::int end                       as occupant_count,
        "Postcode"                                                  as postcode,
        "Rollover"                                                  as rollover,
        "Fire"                                                      as fire,

        _source_resource,
        _ingested_at

    from source

)

select * from renamed
