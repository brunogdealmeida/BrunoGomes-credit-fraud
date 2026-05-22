{{ config(materialized='table') }}

with src as (
    select * from {{ source('bronze', 'tb_fraud_credit') }}
),

agg as (
    select
        count(*)                                                                                                    as total,
        -- completeness
        sum(case when "timestamp"      is null then 1 else 0 end)                                                  as null_timestamp,
        sum(case when sending_address  is null then 1 else 0 end)                                                  as null_sending_address,
        sum(case when receiving_address is null then 1 else 0 end)                                                 as null_receiving_address,
        sum(case when amount           is null then 1 else 0 end)                                                  as null_amount,
        sum(case when transaction_type is null then 1 else 0 end)                                                  as null_transaction_type,
        sum(case when location_region  is null then 1 else 0 end)                                                  as null_location_region,
        sum(case when anomaly          is null then 1 else 0 end)                                                  as null_anomaly,
        sum(case when risk_score       is null then 1 else 0 end)                                                  as null_risk_score,
        sum(case when login_frequency  is null then 1 else 0 end)                                                  as null_login_frequency,
        sum(case when session_duration is null then 1 else 0 end)                                                  as null_session_duration,
        sum(case when purchase_pattern is null then 1 else 0 end)                                                  as null_purchase_pattern,
        sum(case when age_group        is null then 1 else 0 end)                                                  as null_age_group,
        sum(case when ip_prefix        is null then 1 else 0 end)                                                  as null_ip_prefix,
        -- validity
        sum(case when amount <= 0 then 1 else 0 end)                                                               as invalid_amount,
        sum(case when transaction_type not in ('purchase','sale','transfer','phishing','scam')
                 then 1 else 0 end)                                                                                as invalid_transaction_type,
        sum(case when location_region not in ('Africa','Asia','Europe','North America','South America')
                 then 1 else 0 end)                                                                                as invalid_location_region,
        sum(case when anomaly not in ('low_risk','moderate_risk','high_risk')
                 then 1 else 0 end)                                                                                as invalid_anomaly,
        sum(case when risk_score < 0 or risk_score > 100 then 1 else 0 end)                                        as invalid_risk_score
    from src
),

dedup as (
    select count(*) as distinct_combos
    from (
        select sending_address, receiving_address, "timestamp"
        from src
        group by sending_address, receiving_address, "timestamp"
    )
),

checks as (
    select 'completeness' as check_type, 'timestamp'          as check_name, 'timestamp IS NULL'          as description, null_timestamp          as error_count, total as total_records from agg
    union all select 'completeness', 'sending_address',   'sending_address IS NULL',   null_sending_address,   total from agg
    union all select 'completeness', 'receiving_address', 'receiving_address IS NULL', null_receiving_address, total from agg
    union all select 'completeness', 'amount',            'amount IS NULL',            null_amount,            total from agg
    union all select 'completeness', 'transaction_type',  'transaction_type IS NULL',  null_transaction_type,  total from agg
    union all select 'completeness', 'location_region',   'location_region IS NULL',   null_location_region,   total from agg
    union all select 'completeness', 'anomaly',           'anomaly IS NULL',           null_anomaly,           total from agg
    union all select 'completeness', 'risk_score',        'risk_score IS NULL',        null_risk_score,        total from agg
    union all select 'completeness', 'login_frequency',   'login_frequency IS NULL',   null_login_frequency,   total from agg
    union all select 'completeness', 'session_duration',  'session_duration IS NULL',  null_session_duration,  total from agg
    union all select 'completeness', 'purchase_pattern',  'purchase_pattern IS NULL',  null_purchase_pattern,  total from agg
    union all select 'completeness', 'age_group',         'age_group IS NULL',         null_age_group,         total from agg
    union all select 'completeness', 'ip_prefix',         'ip_prefix IS NULL',         null_ip_prefix,         total from agg
    union all select 'validity', 'amount_positive',          'amount must be > 0',                                                                 invalid_amount,           total from agg
    union all select 'validity', 'transaction_type_valid',   'transaction_type must be one of: purchase, sale, transfer, phishing, scam',          invalid_transaction_type, total from agg
    union all select 'validity', 'location_region_valid',    'location_region must be one of: Africa, Asia, Europe, North America, South America', invalid_location_region,  total from agg
    union all select 'validity', 'anomaly_valid',            'anomaly must be one of: low_risk, moderate_risk, high_risk',                         invalid_anomaly,          total from agg
    union all select 'validity', 'risk_score_range',         'risk_score must be between 0 and 100',                                               invalid_risk_score,       total from agg
    union all
    select
        'validity'                                                                      as check_type,
        'no_duplicates'                                                                 as check_name,
        'no duplicate (sending_address, receiving_address, timestamp) rows'             as description,
        agg.total - dedup.distinct_combos                                               as error_count,
        agg.total                                                                       as total_records
    from agg cross join dedup
)

select
    check_type,
    check_name,
    description,
    error_count,
    total_records,
    round(cast(error_count as double) / nullif(total_records, 0) * 100, 2)               as error_pct,
    round((1.0 - cast(error_count as double) / nullif(total_records, 0)) * 100, 2)       as conformance_pct,
    case when error_count = 0 then 'PASS' else 'FAIL' end                                as status
from checks
order by check_type, check_name
