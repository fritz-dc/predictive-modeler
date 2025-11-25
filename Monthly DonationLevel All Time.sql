SELECT
    donor.personid  AS "donor_id",
    (DATE(recurring_payment.createdate )) AS "monthly_subscription_payment_date",
    MAX(CASE WHEN recurring_subscription.is_active  THEN 1 ELSE 0 END) AS "monthly_subscription_active",
    MAX(recurring_subscription_rollups.longest_month_streak)  AS "monthly_subscription_longest_streak",
    MAX(DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', recurring_subscription_rollups.first_joined_at ))) AS "monthly_subscription_joined_date",
    MAX(DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', recurring_subscription_rollups.last_retired_at ))) AS "monthly_subscription_retired_date",
    COALESCE(SUM(recurring_payment.amount ), 0) AS "monthly_subscription_payment_amount"
FROM ${users.SQL_TABLE_NAME} AS donor
LEFT JOIN dbt_target.fct_recurring_subscriptions  AS recurring_subscription ON donor.personid = recurring_subscription.donor_id
LEFT JOIN ${recurring_payment.SQL_TABLE_NAME} AS recurring_payment ON recurring_subscription.recurring_donation_id = recurring_payment.recurringdonation AND recurring_payment.eventtype IN (0,1)
LEFT JOIN dbt_target.fct_donor_recurring_subscriptions  AS recurring_subscription_rollups ON donor.personid = recurring_subscription_rollups.donor_id
GROUP BY
    1,
    2
HAVING COALESCE(SUM(recurring_payment.amount ), 0) > 0
