
{{ config(
    materialized='incremental',
    unique_key= ['walletprofileid', 'profile_type'],
    on_schema_change='append_new_columns'
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'profiles_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% set stg_table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'profiles_stagging')" %}
{% set stg_table_exists_result = run_query(stg_table_exists_query) %}
{% set stg_table_exists =stg_table_exists_result.rows[0][0] if stg_table_exists_result and stg_table_exists_result.rows else False %}


SELECT
    md5(random()::text || '-' || COALESCE(wp.walletprofileid, '') || '-' || COALESCE(wp.updatedat_aibyte_transform::text, '') || '-' || COALESCE(p.lastmodifiedat::text, '') || '-' || now()::text) AS id,
    'insert' AS operation,
    true AS currentflag,
    null::timestamptz AS expdate,
    wp.walletprofileid,
    wp.partnerid,
    md5(
        COALESCE(walletprofileid, '') || '::' || COALESCE(wp.partnerid, '') || '::' || COALESCE(p.partnercode, '') || '::' ||
        COALESCE(wp.type, '') || '::' || COALESCE(wp.fees::text, '') || '::' || COALESCE(wp.limits::text, '') || '::' ||
        COALESCE(wp.vouchers::text, '') || '::' || COALESCE(p.activeflag::text, '') || '::' || COALESCE(p.partnernameen, '') || '::' || 
        COALESCE(p.partnernamear, '') || '::' || COALESCE(courierpartnerid, '') 
    ) AS hash_column,
    (wp.createdat_aibyte_transform::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as profile_createdat_utc2,
    (wp.updatedat_aibyte_transform::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as profile_modifiedat_utc2,
    (wp.deletedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as profile_deletedat_utc2,

    wp.type as profile_type,
    p.partnernameen as partner_name_en,
    p.partnernamear as partner_name_ar,
    activeflag as partner_isactive,

    (p.createdat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as partner_createdat_utc2,
    (p.lastmodifiedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as partner_modifiedat_utc2,
    (p.deletedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as partner_deletedat_utc2,

    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as loaddate

FROM {{source('axis_core', 'walletprofiles')}} wp
LEFT JOIN {{source('axis_kyc', 'partner')}} p on wp.partnerid = p.partnerid

{% if is_incremental() and table_exists and stg_table_exists %}
    WHERE wp._airbyte_emitted_at > COALESCE((SELECT max(loaddate::timestamptz) FROM {{ source('dbt-dimensions', 'profiles_dimension') }}), '1900-01-01'::timestamp)
        OR p._airbyte_emitted_at > COALESCE((SELECT max(loaddate::timestamptz) FROM {{ source('dbt-dimensions', 'profiles_dimension') }}), '1900-01-01'::timestamp)
{% endif %}
