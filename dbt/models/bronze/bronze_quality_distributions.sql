{{ config(materialized='table') }}

with src as (
    select * from {{ source('bronze', 'tb_fraud_credit') }}
),

total as (
    select count(*) as total_records from src
),

anomaly_dist as (
    select 'anomaly' as dimension, coalesce(cast(anomaly as varchar), '[NULL]') as dim_value, count(*) as record_count
    from src group by anomaly
),

type_dist as (
    select 'transaction_type' as dimension, coalesce(cast(transaction_type as varchar), '[NULL]') as dim_value, count(*) as record_count
    from src group by transaction_type
),

region_dist as (
    select 'location_region' as dimension, coalesce(cast(location_region as varchar), '[NULL]') as dim_value, count(*) as record_count
    from src group by location_region
),

age_dist as (
    select 'age_group' as dimension, coalesce(cast(age_group as varchar), '[NULL]') as dim_value, count(*) as record_count
    from src group by age_group
),

pattern_dist as (
    select 'purchase_pattern' as dimension, coalesce(cast(purchase_pattern as varchar), '[NULL]') as dim_value, count(*) as record_count
    from src group by purchase_pattern
),

risk_bands as (
    select
        'risk_score_band' as dimension,
        case
            when risk_score is null then '[NULL]'
            when risk_score <= 25   then '0-25'
            when risk_score <= 50   then '26-50'
            when risk_score <= 75   then '51-75'
            else                         '76-100'
        end as dim_value,
        count(*) as record_count
    from src
    group by
        case
            when risk_score is null then '[NULL]'
            when risk_score <= 25   then '0-25'
            when risk_score <= 50   then '26-50'
            when risk_score <= 75   then '51-75'
            else                         '76-100'
        end
),

all_dist as (
    select * from anomaly_dist
    union all select * from type_dist
    union all select * from region_dist
    union all select * from age_dist
    union all select * from pattern_dist
    union all select * from risk_bands
)

select
    d.dimension,
    d.dim_value,
    d.record_count,
    round(cast(d.record_count as double) / t.total_records * 100, 2) as pct
from all_dist d
cross join total t
order by dimension, record_count desc
