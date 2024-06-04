
{{ config(
    materialized='incremental',
    unique_key= ['walletprofileid', 'profile_type'],
    on_schema_change='create'
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'profiles_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% if table_exists %}

with update_old as (
    SELECT
        stg.id AS id,
        CASE
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column AND final.operation = 'insert' THEN 'update'
            ELSE 'exp'
        END AS operation,
        CASE
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN true
            ELSE false
        END AS currentflag,
        CASE
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN null::timestamptz
            ELSE now()::timestamptz
        END AS expdate,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.walletprofileid
            ELSE final.walletprofileid
        END AS walletprofileid,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.partnerid
            ELSE final.partnerid
        END AS partnerid,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.hash_column
            ELSE final.hash_column
        END AS hash_column,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.profile_createdat_utc2
            ELSE final.profile_createdat_utc2
        END AS profile_createdat_utc2,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.profile_modifiedat_utc2
            ELSE final.profile_modifiedat_utc2
        END AS profile_modifiedat_utc2,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.profile_deletedat_utc2
            ELSE final.profile_deletedat_utc2
        END AS profile_deletedat_utc2,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.profile_type
            ELSE final.profile_type
        END AS profile_type,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.partner_name_en
            ELSE final.partner_name_en
        END AS partner_name_en,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.partner_name_ar
            ELSE final.partner_name_ar
        END AS partner_name_ar,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.partner_isactive
            ELSE final.partner_isactive
        END AS partner_isactive,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.partner_createdat_utc2
            ELSE final.partner_createdat_utc2
        END AS partner_createdat_utc2,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.partner_modifiedat_utc2
            ELSE final.partner_modifiedat_utc2
        END AS partner_modifiedat_utc2,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.partner_deletedat_utc2
            ELSE final.partner_deletedat_utc2
        END AS partner_deletedat_utc2,
        (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS loaddate  

    FROM {{ source('dbt-dimensions', 'profiles_stagging') }} stg
    JOIN {{ source('dbt-dimensions', 'profiles_dimension')}} final
        ON stg.walletprofileid = final.walletprofileid AND stg.profile_type = final.profile_type
    WHERE final.hash_column is not null and final.operation != 'exp'
        AND stg.loaddate > final.loaddate
)

SELECT * from update_old

{% else %}

SELECT *
FROM {{ source('dbt-dimensions', 'profiles_stagging') }} stg
WHERE stg.loaddate > '2050-01-01'::timestamptz

{% endif %}

