-- Conformed age-band dimension. The band definitions come from the ABS CL_AGE
-- codelist (via seed, verified against the codelist API); the SAME bands are
-- used by fact_population_annual (native grain) and fact_crash_casualties
-- (exact ages bucketed via min/max), so casualty rates per age group divide
-- cleanly by the matching population.

select
    age_band_code,
    age_band,
    min_age,
    max_age,
    sort_order
from {{ ref('seed_abs_age_bands') }}
