SELECT 
user_id AS "donor_id",
(TO_CHAR(DATE_TRUNC('month', CONVERT_TIMEZONE('UTC', 'America/New_York', event_at)), 'YYYY-MM')) AS "share_sent_month",
COUNT(event_id) AS "share_event_count"
FROM dbt_target.fct_project_share_events
GROUP BY 
    1,
    2
