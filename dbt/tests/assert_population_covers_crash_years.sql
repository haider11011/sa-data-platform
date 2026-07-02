-- Every year with crash data must also have population data. The per-capita
-- rates view inner-joins the two facts; without this guard, a lapsed ABS
-- ingest would silently shrink the view instead of failing the build.

with crash_years as (

    select distinct extract(year from crash_date)::int as year
    from {{ ref('fact_road_crashes') }}

),

population_years as (

    select distinct year
    from {{ ref('fact_population_annual') }}

)

select crash_years.year
from crash_years
left join population_years using (year)
where population_years.year is null
