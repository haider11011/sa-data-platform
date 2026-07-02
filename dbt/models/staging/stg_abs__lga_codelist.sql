-- Silver-layer view over the ABS LGA_2024 codelist, filtered to SA.
-- This is the authoritative code -> name mapping used to conform the
-- free-text LGA names in the crash data to stable ABS LGA codes.

with source as (

    select * from {{ source('abs', 'abs_lga_codelist') }}

),

renamed as (

    select
        code                                            as lga_code,
        -- ABS disambiguates cross-state name clashes with a state suffix,
        -- e.g. "Campbelltown (SA)"; within an SA-only platform the suffix is
        -- noise and would break name matching against the crash data.
        trim(regexp_replace(name, '\s*\(SA\)$', ''))    as lga_name,
        name                                            as lga_name_abs

    from source
    where code like '4%'
      and code <> '4'  -- bare '4' is the South Australia state total, not an LGA

)

select * from renamed
