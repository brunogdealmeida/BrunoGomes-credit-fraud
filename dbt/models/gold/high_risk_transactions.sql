{{ config(materialized="view") }}

/*
  Individual high-risk transactions: anomaly = high_risk OR risk_score >= 75.
  Shown in the transactions detail table on the dashboard.
*/
SELECT
    transaction_datetime,
    transaction_date,
    sending_address,
    receiving_address,
    CAST(amount AS DOUBLE)      AS amount,
    amount_tier,
    transaction_type,
    location_region,
    ip_prefix,
    CAST(risk_score AS DOUBLE)  AS risk_score,
    anomaly,
    age_group,
    purchase_pattern,
    login_frequency,
    session_duration,
    is_fraudulent
FROM {{ source("silver_layer", "tb_fraud_credit") }}
WHERE
    anomaly = 'high_risk'
    OR CAST(risk_score AS DOUBLE) >= 75.0
ORDER BY
    risk_score DESC,
    transaction_datetime DESC
