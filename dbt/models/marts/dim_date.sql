-- Conformed date dimension, day grain, generated entirely in dbt (no source
-- system owns "time"). The natural date is the primary key — readable in every
-- query and natively understood by Power BI date hierarchies.
--
-- Range covers population history (2015+) through beyond the latest crash
-- extract; regenerated on every run so it never needs manual extension until
-- the end date, which a dbt-expectations test guards.

with spine as (

    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2015-01-01' as date)",
            end_date="cast('2027-01-01' as date)"
        )
    }}

)

select
    date_day::date                                as date_day,
    extract(year from date_day)::int              as year,
    extract(quarter from date_day)::int           as quarter,
    'Q' || extract(quarter from date_day)         as quarter_name,
    extract(month from date_day)::int             as month,
    to_char(date_day, 'FMMonth')                  as month_name,
    extract(isodow from date_day)::int            as day_of_week,
    to_char(date_day, 'FMDay')                    as day_name,
    extract(isodow from date_day) in (6, 7)       as is_weekend,
    -- Australian financial year (July-June): FY2021 runs 2020-07-01..2021-06-30.
    -- Local reporting convention; SA government reporting is FY-based.
    (extract(year from date_day)
        + case when extract(month from date_day) >= 7 then 1 else 0 end)::int
                                                  as financial_year
from spine
