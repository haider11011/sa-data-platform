-- Bronze landing zone. Ingestion scripts write here; dbt only ever reads from
-- it (declared as a dbt "source"). Staging and marts schemas are created by
-- dbt itself so the warehouse layout is fully reproducible from code.
CREATE SCHEMA IF NOT EXISTS raw;
