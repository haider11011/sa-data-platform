{% snapshot region_snapshot %}

{#-
  SCD Type 2 tracking of region attributes. Included primarily to demonstrate
  the pattern: LGA names DO change in the real world (DC Mallala became
  Adelaide Plains Council in 2017, DC Le Hunte became Wudinna DC in 2008), but
  within this project's data window the codelist is stable, so expect a single
  version per LGA. If ABS renames an LGA in a future codelist release, the
  next run closes the old version (dbt_valid_to) and opens a new one, and
  dim_region exposes both with validity ranges.
-#}

{{
    config(
        schema='snapshots',
        unique_key='lga_code',
        strategy='check',
        check_cols=['lga_name'],
    )
}}

select
    lga_code,
    lga_name,
    lga_name_abs
from {{ ref('stg_abs__lga_codelist') }}

{% endsnapshot %}
