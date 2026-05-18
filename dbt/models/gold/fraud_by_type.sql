{{ config(materialized="view") }}

/*
  Transaction volume and risk metrics by transaction type and purchase pattern.
  Highlights which transaction types carry the highest fraud exposure.
*/
SELECT
    transaction_type,
    anomaly,
    purchase_pattern,
    COUNT(*)                                           AS transaction_count,
    ROUND(SUM(CAST(amount AS DOUBLE)), 2)              AS total_amount,
    ROUND(AVG(CAST(amount AS DOUBLE)), 2)              AS avg_amount,
    ROUND(AVG(CAST(risk_score AS DOUBLE)), 2)          AS avg_risk_score,
    ROUND(AVG(CAST(session_duration AS DOUBLE)), 1)    AS avg_session_duration,
    ROUND(AVG(CAST(login_frequency AS DOUBLE)), 2)     AS avg_login_frequency
FROM {{ source("silver_layer", "tb_fraud_credit") }}
GROUP BY
    transaction_type,
    anomaly,
    purchase_pattern
ORDER BY
    transaction_count DESC
