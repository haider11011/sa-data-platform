-- Analysis-ready view: road crash outcomes per 10,000 residents by LGA and
-- year. This is the payoff of the conformed dimensions — two facts from two
-- unrelated source systems (SA Police crash reports, ABS population
-- estimates) divided safely because both resolve to the same region_key.
--
-- Materialised as a view: it's a thin join over already-materialised facts.

{{ config(materialized='view') }}

with crash_outcomes as (

    select
        region_key,
        extract(year from crash_date)::int as year,
        count(*)                           as crashes,
        sum(total_casualties)              as casualties,
        sum(total_fatalities)              as fatalities,
        sum(total_serious_injuries)        as serious_injuries
    from {{ ref('fact_road_crashes') }}
    group by 1, 2

),

population as (

    select
        region_key,
        year,
        sum(population) as population
    from {{ ref('fact_population_annual') }}
    group by 1, 2

)

select
    regions.lga_name,
    regions.lga_code,
    crash_outcomes.year,
    crash_outcomes.crashes,
    crash_outcomes.casualties,
    crash_outcomes.fatalities,
    crash_outcomes.serious_injuries,
    population.population,
    round(crash_outcomes.crashes    * 10000.0 / population.population, 1) as crashes_per_10k,
    round(crash_outcomes.casualties * 10000.0 / population.population, 1) as casualties_per_10k

from crash_outcomes
join population using (region_key, year)   -- inner join: rates only exist where both sides do
join {{ ref('dim_region') }} as regions using (region_key)
where population.population > 0
