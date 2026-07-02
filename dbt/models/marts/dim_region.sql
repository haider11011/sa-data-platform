-- Conformed region dimension (LGA grain), shared by every fact table — the
-- same region_key means the same place whether you're counting crashes or
-- people, which is what makes cross-source ratios (crashes per capita) safe.
--
-- Built from the SCD2 snapshot, so each LGA can carry multiple versions with
-- [valid_from, valid_to) ranges. Facts join on lga_code + event date to pick
-- the version current at the time of the event.

with snapshot_rows as (

    select
        lga_code,
        lga_name,
        lga_name_abs,
        dbt_valid_from,
        dbt_valid_to,
        row_number() over (partition by lga_code order by dbt_valid_from) as version_number
    from {{ ref('region_snapshot') }}

),

versioned as (

    select
        {{ dbt_utils.generate_surrogate_key(['lga_code', 'dbt_valid_from']) }} as region_key,
        lga_code,
        lga_name,
        lga_name_abs,
        -- ABS reserves 9x99-suffixed codes for non-geographic categories
        -- (Unincorporated SA, No Usual Address, Migratory-Offshore-Shipping)
        lga_code not in ('49399', '49499', '49799') as is_incorporated,
        -- The first snapshot run stamps dbt_valid_from with the run timestamp,
        -- but version 1 of each LGA must cover history *before* the platform
        -- first ran, or every pre-existing fact row would fail its date-range
        -- join. Standard SCD2 initialisation: backdate the first version.
        case when version_number = 1
            then '1900-01-01'::date
            else dbt_valid_from::date
        end as valid_from,
        coalesce(dbt_valid_to::date, '9999-12-31'::date) as valid_to,
        dbt_valid_to is null as is_current
    from snapshot_rows

),

-- Explicit unknown member: crashes with no LGA recorded still land in facts
-- with a real (testable) foreign key instead of a NULL.
unknown_member as (

    select
        {{ dbt_utils.generate_surrogate_key(["'UNK'", "'1900-01-01'"]) }} as region_key,
        'UNK'                     as lga_code,
        'Unknown'                 as lga_name,
        'Unknown'                 as lga_name_abs,
        false                     as is_incorporated,
        '1900-01-01'::date        as valid_from,
        '9999-12-31'::date        as valid_to,
        true                      as is_current

)

select * from versioned
union all
select * from unknown_member
