-- Cross-fact reconciliation: the crash fact's total_casualties measure must
-- equal the number of casualty fact rows for that crash. The two facts come
-- from different files in the source archive, so this test proves the archive
-- is internally consistent AND that neither fact dropped or duplicated rows
-- on its way through the pipeline. Returns violating crashes (test passes on
-- zero rows).

select
    crashes.report_id,
    crashes.total_casualties as reported_total,
    count(casualties.report_id) as casualty_rows
from {{ ref('fact_road_crashes') }} as crashes
left join {{ ref('fact_crash_casualties') }} as casualties
    on casualties.report_id = crashes.report_id
group by 1, 2
having crashes.total_casualties <> count(casualties.report_id)
