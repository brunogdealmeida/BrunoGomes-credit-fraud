{{ config(materialized="view") }}

/*
  Daily transaction volume and risk distribution per region.
  Powers the time-series charts in the dashboard.
*/
SELECT
    transaction_date,
    location_region,
    COUNT(*)                                                       AS total_transactions,
    COUNT(CASE WHEN anomaly = 'high_risk'     THEN 1 END)          AS high_risk_count,
    COUNT(CASE WHEN anomaly = 'moderate_risk' THEN 1 END)          AS moderate_risk_count,
    COUNT(CASE WHEN anomaly = 'low_risk'      THEN 1 END)          AS low_risk_count,
    COUNT(CASE WHEN transaction_type IN ('phishing','scam') THEN 1 END) AS fraudulent_count,
    ROUND(SUM(CAST(amount AS DOUBLE)), 2)                          AS total_amount,
    ROUND(AVG(CAST(risk_score AS DOUBLE)), 2)                      AS avg_risk_score,
    ROUND(
        100.0 * COUNT(CASE WHEN anomaly = 'high_risk' THEN 1 END)
        / COUNT(*), 2
    )                                                              AS high_risk_rate_pct
FROM {{ source("silver_layer", "tb_fraud_credit") }}
GROUP BY
    transaction_date,
    location_region
ORDER BY
    transaction_date DESC,
    location_region
