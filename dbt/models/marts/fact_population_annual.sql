-- Fact table (periodic snapshot). Grain: LGA x YEAR x SEX x AGE BAND.
--
-- Only the additive base grain is stored: sex in (Male, Female) and real age
-- bands. The published totals (sex = Persons, age = TOT) are excluded because
-- they are derivable — storing them alongside the parts invites double
-- counting in BI tools. Additivity was verified against the source: the sum
-- of the base cells reproduces the published totals exactly.
--
-- Date convention: an annual observation is anchored to 1 January of its year
-- (ABS ERP is a 30 June estimate, but the anchor exists purely to join
-- dim_date at a consistent day grain across all facts; the year is the truth).

with erp as (

    select * from {{ ref('stg_abs__erp_lga') }}
    where sex in ('Male', 'Female')
      and age_band_code <> 'TOT'

),

regions as (

    select * from {{ ref('dim_region') }}

)

select
    regions.region_key,
    make_date(erp.year, 1, 1) as year_date,
    erp.year,
    erp.sex,
    erp.age_band_code,

    erp.population

from erp
join regions
    on regions.lga_code = erp.lga_code
    and make_date(erp.year, 1, 1) >= regions.valid_from
    and make_date(erp.year, 1, 1) < regions.valid_to
