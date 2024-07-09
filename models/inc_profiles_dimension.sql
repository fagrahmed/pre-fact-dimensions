

{{
    config(
        materialized="incremental",
        unique_key= "hash_column",
        on_schema_change='append_new_columns'
    )
}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_profiles_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

WITH upd_exp_rec AS (
    SELECT
        id,
        operation,
        currentflag,
        expdate,
        walletprofileid,
        partnerid,
        hash_column,
        profile_createdat_local,
        profile_modifiedat_local,
        profile_deletedat_local,
        profile_type,
        partner_name_en,
        partner_name_ar,
        partner_isactive,
        partner_createdat_local,
        partner_modifiedat_local,
        partner_deletedat_local,
        utc,
        loaddate

    FROM {{ ref('inc_profiles_stg_update') }} 

    UNION ALL

    SELECT
        id,
        operation,
        currentflag,
        expdate,
        walletprofileid,
        partnerid,
        hash_column,
        profile_createdat_local,
        profile_modifiedat_local,
        profile_deletedat_local,
        profile_type,
        partner_name_en,
        partner_name_ar,
        partner_isactive,
        partner_createdat_local,
        partner_modifiedat_local,
        partner_deletedat_local,
        utc,
        loaddate

    FROM {{ ref('inc_profiles_stg_exp') }} 
)

{% if table_exists %}

, remove_old_from_dim AS (
	SELECT
        old_rec.id,
        old_rec.operation,
        old_rec.currentflag,
        old_rec.expdate,
        old_rec.walletprofileid,
        old_rec.partnerid,
        old_rec.hash_column,
        old_rec.profile_createdat_local,
        old_rec.profile_modifiedat_local,
        old_rec.profile_deletedat_local,
        old_rec.profile_type,
        old_rec.partner_name_en,
        old_rec.partner_name_ar,
        old_rec.partner_isactive,
        old_rec.partner_createdat_local,
        old_rec.partner_modifiedat_local,
        old_rec.partner_deletedat_local,
        old_rec.utc,
        old_rec.loaddate

    FROM {{ this }} as old_rec
    LEFT JOIN upd_exp_rec ON old_rec.id = upd_exp_rec.id
    WHERE upd_exp_rec.id IS NULL
)

SELECT

    id,
    operation,
    currentflag,
    expdate,
    walletprofileid,
    partnerid,
    hash_column,
    profile_createdat_local,
    profile_modifiedat_local,
    profile_deletedat_local,
    profile_type,
    partner_name_en,
    partner_name_ar,
    partner_isactive,
    partner_createdat_local,
    partner_modifiedat_local,
    partner_deletedat_local,
    utc,
    loaddate 

FROM remove_old_from_dim

UNION ALL

SELECT

    id,
    operation,
    currentflag,
    expdate,
    walletprofileid,
    partnerid,
    hash_column,
    profile_createdat_local,
    profile_modifiedat_local,
    profile_deletedat_local,
    profile_type,
    partner_name_en,
    partner_name_ar,
    partner_isactive,
    partner_createdat_local,
    partner_modifiedat_local,
    partner_deletedat_local,
    utc,
    loaddate

FROM upd_exp_rec

UNION ALL

{% endif %}

SELECT
            id,
        operation,
        currentflag,
        expdate,
        walletprofileid,
        partnerid,
        hash_column,
        profile_createdat_local,
        profile_modifiedat_local,
        profile_deletedat_local,
        profile_type,
        partner_name_en,
        partner_name_ar,
        partner_isactive,
        partner_createdat_local,
        partner_modifiedat_local,
        partner_deletedat_local,
        utc,
        loaddate
    
FROM {{ ref('inc_profiles_stg_new') }}