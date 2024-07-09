{{ config(
    materialized='incremental',
    unique_key= ['walletprofileid', 'profile_type'],
    on_schema_change='append_new_columns',
    pre_hook=[
        "{% if target.schema == 'dbt-dimensions' and source('dbt-dimensions', 'inc_profiles_stg_exp') is not none %}TRUNCATE TABLE {{ source('dbt-dimensions', 'inc_profiles_stg_exp) }};{% endif %}"
    ]
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_profiles_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% if table_exists %}

SELECT  
    final.id,
    'exp' AS operation,
    false AS currentflag,
    (now()::timestamp AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS expdate,
    stg.walletprofileid,
    stg.partnerid,
    stg.hash_column,
    stg.profile_createdat_local,
    stg.profile_modifiedat_local,
    stg.profile_deletedat_local,
    stg.profile_type,
    stg.partner_name_en,
    stg.partner_name_ar,
    stg.partner_isactive,
    stg.partner_createdat_local,
    stg.partner_modifiedat_local,
    stg.partner_deletedat_local,
    stg.utc,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS loaddate

FROM {{ source('dbt-dimensions', 'inc_profiles_stg') }} stg
JOIN {{ source('dbt-dimensions', 'inc_profiles_dimension') }} final ON stg.walletprofileid = final.walletprofileid AND stg.profile_type = final.profile_type
WHERE stg.loaddate > final.loaddate AND final.hash_column != stg.hash_column 

{% else %}

SELECT
    stg.id,
    stg.operation,
    stg.currentflag,
    stg.expdate,
    stg.walletprofileid,
    stg.partnerid,
    stg.hash_column,
    stg.profile_createdat_local,
    stg.profile_modifiedat_local,
    stg.profile_deletedat_local,
    stg.profile_type,
    stg.partner_name_en,
    stg.partner_name_ar,
    stg.partner_isactive,
    stg.partner_createdat_local,
    stg.partner_modifiedat_local,
    stg.partner_deletedat_local,
    stg.utc,
    stg.loaddate

FROM {{ source('dbt-dimensions', 'inc_profiles_stg') }} stg
WHERE stg.loaddate > '2050-01-01'::timestamptz 

{% endif %}
