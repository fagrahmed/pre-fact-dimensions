-- models/profiles_dimension.sql

{{config(materialized='table') }}

SELECT
    wp.walletprofileid,
    wp.partnerid,

    (wp.createdat_aibyte_transform::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as profile_createdat_utc2,
    (wp.updatedat_aibyte_transform::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as profile_modifiedat_utc2,
    (wp.deletedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as profile_deletedat_utc2,

    wp.type as profile_type,
    p.partnernameen as partner_name,
    activeflag as partner_isactive,

    (p.createdat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as partner_createdat_utc2,
    (p.lastmodifiedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as partner_modifiedat_utc2,
    (p.deletedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as partner_deletedat_utc2,


    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as loaddate,
    null::timestamptz as expdate,
    true::boolean as currentflag

FROM {{source('axis_core', 'walletprofiles')}} wp
LEFT JOIN {{source('axis_kyc', 'partner')}} p on wp.partnerid = p.partnerid
