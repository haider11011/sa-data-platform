-- The conformance bridge between the two source systems' region vocabularies.
--
-- The crash data carries free-text council names ("CITY OF SALISBURY",
-- "DC MT.BARKER.", "CC PT.AUGUSTA."); ABS publishes population against coded
-- LGAs ("Salisbury" = 47140). dim_region can only be a genuinely *conformed*
-- dimension if every crash LGA name resolves to an ABS LGA code.
--
-- Strategy, in order:
--   1. a deterministic normalisation rule chain (below) that handles the
--      systematic abbreviations, and
--   2. seed_lga_name_overrides for the handful of names that are historic
--      (renamed councils), misspelled, or too irregular for rules.
-- Anything still unmatched fails the not_null test on lga_code — new source
-- names surface as a test failure, never as silently dropped fact rows.

with crash_names as (

    select distinct lga_name as source_lga_name
    from {{ ref('stg_data_sa__crashes') }}
    where lga_name is not null

),

normalised as (

    select
        source_lga_name,
        trim(
            regexp_replace(   -- 5. strip council-type suffixes
                regexp_replace(   -- 4. strip council-type prefixes
                    btrim(regexp_replace(   -- 3. collapse and trim whitespace
                        translate(    -- 2. expand punctuation: & , / and stray periods
                            replace(replace(upper(source_lga_name), 'PT.', 'PORT '), 'MT.', 'MOUNT '),
                            '&,/.', '    '
                        ),
                        '\s+', ' ', 'g'
                    )),
                    '^(CC OF |CC |DC OF |DC |CT |MC |RC |THE CITY OF |CITY OF |TOWN OF |DISTRICT COUNCIL OF |REGIONAL COUNCIL OF |MUNICIPAL COUNCIL OF |RURAL CITY OF |THE )',
                    ''
                ),
                '( DISTRICT COUNCIL| REGIONAL COUNCIL| TOWN COUNCIL| CITY COUNCIL| COUNCIL| DISTRICT| REGIONAL| MUNICIPALITY)$',
                ''
            )
        ) as normalised_name
    from crash_names

),

-- '&' -> ' ' via translate() means "NORWOOD,PAYNEHAM & ST PETERS" collapses to
-- "NORWOOD PAYNEHAM ST PETERS", but ABS keeps that AND ("Norwood Payneham And
-- St Peters") while dropping it elsewhere ("Naracoorte Lucindale") — so we
-- match with the conjunction removed from BOTH sides.
codelist as (

    select
        lga_code,
        upper(lga_name)                                   as abs_name,
        regexp_replace(upper(lga_name), ' AND ', ' ', 'g') as abs_name_no_conjunction
    from {{ ref('stg_abs__lga_codelist') }}

),

matched as (

    select
        n.source_lga_name,
        n.normalised_name,
        coalesce(
            override.lga_code,
            by_name.lga_code,
            by_name_nc.lga_code
        ) as lga_code,
        case
            when override.lga_code is not null then 'seed_override'
            when by_name.lga_code is not null then 'exact_name'
            when by_name_nc.lga_code is not null then 'name_without_conjunction'
        end as match_method
    from normalised as n
    left join {{ ref('seed_lga_name_overrides') }} as override
        on override.source_lga_name = n.source_lga_name
    left join codelist as by_name
        on by_name.abs_name = n.normalised_name
    left join codelist as by_name_nc
        on by_name_nc.abs_name_no_conjunction
         = regexp_replace(n.normalised_name, ' AND ', ' ', 'g')

)

select * from matched
