WITH
eligible_donors AS (
SELECT donor_id
FROM (
    SELECT
    donor.personid AS donor_id,
    COALESCE(
        CAST((
        SUM(DISTINCT (
            CAST(FLOOR(COALESCE(CASE WHEN payment.is_green THEN payment.amount END, 0) * (1000000 * 1.0)) AS DECIMAL(38,0))
            + CAST(STRTOL(LEFT(MD5(CAST(payment.payment_id AS VARCHAR)),15),16) AS DECIMAL(38,0)) * 1.0e8
            + CAST(STRTOL(RIGHT(MD5(CAST(payment.payment_id AS VARCHAR)),15),16) AS DECIMAL(38,0))
        ))
        - SUM(DISTINCT
            CAST(STRTOL(LEFT(MD5(CAST(payment.payment_id AS VARCHAR)),15),16) AS DECIMAL(38,0)) * 1.0e8
            + CAST(STRTOL(RIGHT(MD5(CAST(payment.payment_id AS VARCHAR)),15),16) AS DECIMAL(38,0))
        )
        ) AS DOUBLE PRECISION) / CAST((1000000 * 1.0) AS DOUBLE PRECISION),
        0
    ) AS greendollars_total_amount_12mo,
    COUNT(DISTINCT CASE WHEN payment.is_green THEN COALESCE(payment.donation_id, payment.gift_purchase_id) END) AS green_count_12mo
    FROM ${users.SQL_TABLE_NAME} AS donor
    LEFT JOIN dcdonor AS donor_ ON donor.personid = donor_.donorid
    LEFT JOIN dbt_target.fct_donor_labels AS donor_labels ON donor.personid = donor_labels.donor_id
    LEFT JOIN dbt_target.fct_donors AS donor_facts ON donor.personid = donor_facts.donor_id
    LEFT JOIN dbt_target.fct_payment AS payment ON payment.donor_id = donor.personid
    LEFT JOIN giftpurchase AS gift_purchase ON payment.gift_purchase_id = gift_purchase.id
    LEFT JOIN giving_cart_item AS gift_purchase_cart_item ON gift_purchase.cart_item = gift_purchase_cart_item.id
    LEFT JOIN dbt_target.fct_projects AS project ON project.project_id = COALESCE(payment.project_id, gift_purchase_cart_item.intended_project)
    LEFT JOIN school AS school ON school.schoolid = project.school_id
    LEFT JOIN address AS school_address ON school.addressid = school_address.addressid
    WHERE
    (NOT (donor_labels.grant_account) OR donor_labels.grant_account IS NULL)
    AND payment.received_at >= DATEADD(year, -3, GETDATE()) 
    AND (NOT (project.is_one_time_sponsor_donor_a_government_entity) OR project.is_one_time_sponsor_donor_a_government_entity IS NULL)
    AND (NOT ((UPPER(school_address.state)) = 'PR') OR ((UPPER(school_address.state)) = 'PR') IS NULL)
    GROUP BY 1
) AS payment
WHERE
    (green_count_12mo >= 8 AND greendollars_total_amount_12mo >= 100)
    OR (green_count_12mo >= 4 AND greendollars_total_amount_12mo >= 200)
    OR (green_count_12mo >= 2 AND greendollars_total_amount_12mo >= 500)
    OR (green_count_12mo >= 1 AND greendollars_total_amount_12mo >= 1000)
)

SELECT
    donor.personid  AS "donor_id",
    MAX(CASE WHEN donor_labels.first_cart_is_teacher_referred  THEN 'Yes' ELSE 'No' END) AS "teacher_referred",
    MAX(CASE WHEN teacher_donor.teacherid IS NOT NULL  THEN 'Yes' ELSE 'No' END) AS "is_teacher",
    COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE( case when  payment.is_green   then  payment.amount   end  ,0)*(1000000*1.0)) AS DECIMAL(38,0))) + CAST(STRTOL(LEFT(MD5(CAST( payment.payment_id   AS VARCHAR)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CAST( payment.payment_id   AS VARCHAR)),15),16) AS DECIMAL(38,0)) ) - SUM(DISTINCT CAST(STRTOL(LEFT(MD5(CAST( payment.payment_id   AS VARCHAR)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CAST( payment.payment_id   AS VARCHAR)),15),16) AS DECIMAL(38,0))) )  AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0) AS "greendollars_total_amount",
    COUNT(DISTINCT case when  payment.is_green   then coalesce(payment.donation_id, payment.gift_purchase_id) end ) AS "payment_green_count",
    COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE( recurring_payment.amount  ,0)*(1000000*1.0)) AS DECIMAL(38,0))) + CAST(STRTOL(LEFT(MD5(CAST( recurring_payment.id  AS VARCHAR)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CAST( recurring_payment.id  AS VARCHAR)),15),16) AS DECIMAL(38,0)) ) - SUM(DISTINCT CAST(STRTOL(LEFT(MD5(CAST( recurring_payment.id  AS VARCHAR)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CAST( recurring_payment.id  AS VARCHAR)),15),16) AS DECIMAL(38,0))) )  AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0) AS "monthly_total_amount"
FROM ${users.SQL_TABLE_NAME} AS donor
LEFT JOIN dcteacher  AS teacher_donor ON donor.personid  = teacher_donor.teacherid
LEFT JOIN dbt_target.fct_donor_labels  AS donor_labels ON donor.personid  = donor_labels.donor_id
LEFT JOIN dbt_target.fct_payment  AS payment ON payment.donor_id = donor.personid
LEFT JOIN giftpurchase  AS gift_purchase ON payment.gift_purchase_id  = gift_purchase.id
LEFT JOIN giving_cart_item  AS gift_purchase_cart_item ON gift_purchase.cart_item  = gift_purchase_cart_item.id
LEFT JOIN dbt_target.fct_projects  AS project ON project.project_id = coalesce(payment.project_id, gift_purchase_cart_item.intended_project)
LEFT JOIN dbt_target.fct_recurring_subscriptions  AS recurring_subscription ON donor.personid = recurring_subscription.donor_id
LEFT JOIN ${recurring_payment.SQL_TABLE_NAME} AS recurring_payment ON recurring_subscription.recurring_donation_id = recurring_payment.recurringdonation AND recurring_payment.eventtype IN (0,1)
WHERE (NOT (donor_labels.grant_account ) OR (donor_labels.grant_account ) IS NULL) AND (payment.is_green ) AND (NOT (project.is_one_time_sponsor_donor_a_government_entity ) OR (project.is_one_time_sponsor_donor_a_government_entity ) IS NULL)
  AND DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', payment.received_at)) >= DATEADD(year, -3, CURRENT_DATE)
GROUP BY
    1
