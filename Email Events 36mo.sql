SELECT
    users.personid  AS "donor_id",
    email_facts.type  AS "email_type",
    (TO_CHAR(DATE_TRUNC('month', CONVERT_TIMEZONE('UTC', 'America/New_York', email_facts.sent_at )), 'YYYY-MM')) AS "email_sent_month",
    COUNT(DISTINCT email_facts.email_id ) AS "email_sent_count",
    COUNT(DISTINCT CASE WHEN ( (TO_CHAR(DATE_TRUNC('second', CONVERT_TIMEZONE('UTC', 'America/New_York', email_facts.last_opened_at )), 'YYYY-MM-DD HH24:MI:SS')) ) IS NOT NULL
                             THEN  email_facts.email_id
                             END)  AS "email_open_count",
    COUNT(DISTINCT CASE WHEN ( (TO_CHAR(DATE_TRUNC('second', CONVERT_TIMEZONE('UTC', 'America/New_York', email_facts.last_clicked_at )), 'YYYY-MM-DD HH24:MI:SS')) ) IS NOT NULL
                             THEN  email_facts.email_id
                             END)  AS "email_click_count"
FROM dbt_target.fct_emails  AS email_facts
LEFT JOIN ${users.SQL_TABLE_NAME} AS users ON email_facts.user_id  = users.personid
WHERE email_facts.sent_at >= DATEADD(year, -3, GETDATE()) 
GROUP BY
    (DATE_TRUNC('month', CONVERT_TIMEZONE('UTC', 'America/New_York', email_facts.sent_at ))),
    1,
    2