{{ config(materialized="view") }}

/*
  Aggregated fraud metrics per region and anomaly label.
  Used by Superset for the regional breakdown dashboard charts.
*/
SELECT
    location_region,
    anomaly,
    COUNT(*)                                           AS transaction_count,
    ROUND(SUM(CAST(amount AS DOUBLE)), 2)              AS total_amount,
    ROUND(AVG(CAST(amount AS DOUBLE)), 2)              AS avg_amount,
    ROUND(AVG(CAST(risk_score AS DOUBLE)), 2)          AS avg_risk_score,
    MAX(CAST(amount AS DOUBLE))                        AS max_amount,
    COUNT(CASE WHEN transaction_type IN ('phishing','scam') THEN 1 END) AS fraudulent_count,
    ROUND(
        100.0 * COUNT(CASE WHEN transaction_type IN ('phishing','scam') THEN 1 END)
        / COUNT(*), 2
    )                                                  AS fraud_rate_pct
FROM {{ source("silver_layer", "tb_fraud_credit") }}
GROUP BY
    location_region,
    anomaly
ORDER BY
    location_region,
    anomaly
