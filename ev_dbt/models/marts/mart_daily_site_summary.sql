SELECT
    DATE(connection_time) AS session_date,
    site_id,
    COUNT(*) AS total_sessions,
    COUNT(DISTINCT station_id) AS active_stations,
    ROUND(SUM(kwh_delivered)::numeric, 3) AS total_kwh_delivered,
    ROUND(
        AVG(EXTRACT(EPOCH FROM (disconnect_time - connection_time)) / 60)::numeric,
        2
    ) AS avg_session_duration_minutes
FROM {{ ref('stg_charging_sessions') }}
WHERE connection_time IS NOT NULL
  AND disconnect_time IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2