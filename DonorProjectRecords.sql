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
    (DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', payment.received_at ))) AS "payment_date",
    cart_window_facts.donor_cart_sequence_number AS "donation_n",
    CASE WHEN payment_received_date.is_big_event  THEN 1 ELSE 0 END AS "payment_on_big_event",
    CASE WHEN donor.subscribed_to_marketing_emails = 1
        AND (sf_donor_contact.top_donor_unsubscribe_c IS NULL OR NOT sf_donor_contact.top_donor_unsubscribe_c)
        AND (sf_donor_contact.do_not_contact_c        IS NULL OR NOT sf_donor_contact.do_not_contact_c)
        AND (sf_donor_contact.major_gift_prospect_status_c IS NULL
            OR (
                sf_donor_contact.major_gift_prospect_status_c NOT ILIKE 'Converted to Major Donor'
                AND sf_donor_contact.major_gift_prospect_status_c NOT ILIKE 'Outreach Initiated'))
    THEN 1 ELSE 0 END AS "subscribed_to_marketing_emails",
    CASE WHEN donor_labels.first_cart_is_teacher_referred  THEN 1 ELSE 0 END AS "is_teacher_referred",
    CASE WHEN teacher_donor.teacherid IS NOT NULL  THEN 1 ELSE 0 END AS "is_teacher",
    CASE WHEN donor_.hasrecurringdonation  THEN 1 ELSE 0 END AS "is_monthly_donor",
    CASE WHEN sf_donor_contact.major_gift_prospect_status_c ILIKE 'Converted to Major Donor' THEN 1 ELSE 0 END AS "major_gift_donor",
    CASE WHEN checks_received.received_check_type IN ('DONOR_ADVISED_FUND','IRA','CHARIOT_DAF') THEN 1 ELSE 0 END AS "daf_payment",
    teacher.personid  AS "teacher_id",
    school.schoolid  AS "school_id",
    SUBSTRING(school_address.zip, 1, 5)  AS "school_zip",
    CASE WHEN school_address.latitude  IS NOT NULL AND school_address.longitude  IS NOT NULL THEN (
COALESCE(CAST(school_address.latitude  AS VARCHAR),'') || ',' ||
COALESCE(CAST(school_address.longitude  AS VARCHAR),'')) ELSE NULL END
AS "school_lat_long",
    SUBSTRING(donor_address.zip, 1, 5)  AS "donor_zip",
CASE WHEN donor_location.internal_point_latitude  IS NOT NULL AND donor_location.internal_point_longitude  IS NOT NULL THEN (
COALESCE(CAST(donor_location.internal_point_latitude  AS VARCHAR),'') || ',' ||
COALESCE(CAST(donor_location.internal_point_longitude  AS VARCHAR),'')) ELSE NULL END 
AS "donor_lat_long",
    project_labels.category AS "project_category",
    CASE WHEN donation_facts.donation_sequence = 1 THEN 1 ELSE 0 END AS "gift_is_projects_first",
    CASE WHEN donation_facts.donation_reverse_sequence = 1  THEN 1 ELSE 0 END AS "gift_is_projects_last",
    CASE WHEN payment.donation_type <> 'donation' THEN 1 ELSE 0 END AS "gift_card_purchase",
    CASE
WHEN (donation.feedbackrequired = 'Y')  THEN 'Opted in'
WHEN donation_facts.is_sty_offered = TRUE AND (donation.feedbackrequired = 'Y') = FALSE   THEN 'Opted out'
WHEN donation_facts.is_sty_eligible = FALSE  THEN 'Not eligible'
END AS "sty_status",
    donation.optionaldonationrate  AS "optional_donation_rate",
    dyi_citizen_match.matchimpactmultiple AS "match_xyi_multiplier",
    CASE
WHEN project_labels.grade = 'Grades PreK-2'  THEN 'Grades PreK-2'
WHEN project_labels.grade = 'Grades 3-5'  THEN 'Grades 3-5'
WHEN project_labels.grade = 'Grades 6-8'  THEN 'Grades 6-8'
WHEN project_labels.grade = 'Grades 9-12'  THEN 'Grades 9-12'
WHEN project_labels.grade is null  THEN ''
ELSE 'unknown'
END AS "project_grade",
    CASE
WHEN referral.source IS NULL THEN 'unattributed'
WHEN referral.source ILIKE 'dc' THEN 'sharetools'
WHEN referral.medium ILIKE 'email' THEN 'email'
WHEN referral.medium ILIKE 'ad' THEN 'ad'
ELSE 'other'
END AS "referral_source",
    referral.medium AS "referral_medium",
    teacher_facts.lifetime_fully_funded_projects AS "teacher_lifetime_projects_fully_funded",
    teacher_facts.lifetime_number_of_donations_received AS "teacher_lifetime_donations",
    CASE WHEN project.funded_at IS NOT NULL OR project.is_essentials_list THEN 1 ELSE 0 END AS "project_got_fully_funded",
    project.total_cost AS "project_total_cost",
    CASE WHEN project.essentials_list_id IS NOT NULL THEN 1 ELSE 0 END AS "is_classroom_essentials_list",
    CASE WHEN donation.anonymous THEN 1 ELSE 0 END AS "donation_is_anonymous",
    CASE
WHEN payment.payment_type_detailed IN ('cc-amex','cc-visa','cc-mastercard','cc-discover','cc-google','cc-diners','cc-jcb','cc-unionpay','cc-legacy') THEN 'credit'
WHEN payment.payment_type_detailed = 'debit' THEN 'giftcard'
WHEN payment.payment_type_detailed IN ('amazon','paypal','check') THEN payment.payment_type_detailed
ELSE 'OTHER'
END AS "payment_type",
    payment.is_green AS "is_green_payment",
COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE( payment.amount  ,0)*(1000000*1.0)) AS DECIMAL(38,0))) + CAST(STRTOL(LEFT(MD5(CAST( payment.payment_id   AS VARCHAR)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CAST( payment.payment_id   AS VARCHAR)),15),16) AS DECIMAL(38,0)) ) - SUM(DISTINCT CAST(STRTOL(LEFT(MD5(CAST( payment.payment_id   AS VARCHAR)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CAST( payment.payment_id   AS VARCHAR)),15),16) AS DECIMAL(38,0))) )  AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0) AS "payment_amount",
COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE( case when  payment.is_green   then  payment.amount   end  ,0)*(1000000*1.0)) AS DECIMAL(38,0))) + CAST(STRTOL(LEFT(MD5(CAST( payment.payment_id   AS VARCHAR)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CAST( payment.payment_id   AS VARCHAR)),15),16) AS DECIMAL(38,0)) ) - SUM(DISTINCT CAST(STRTOL(LEFT(MD5(CAST( payment.payment_id   AS VARCHAR)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CAST( payment.payment_id   AS VARCHAR)),15),16) AS DECIMAL(38,0))) )  AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0) AS "green_payment_amount",
COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE( donor_credit_facts.available_balance ,0)*(1000000*1.0)) AS DECIMAL(38,0))) + CAST(STRTOL(LEFT(MD5(CAST( donor_credit_facts.donor_id   AS VARCHAR)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CAST( donor_credit_facts.donor_id   AS VARCHAR)),15),16) AS DECIMAL(38,0)) ) - SUM(DISTINCT CAST(STRTOL(LEFT(MD5(CAST( donor_credit_facts.donor_id   AS VARCHAR)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CAST( donor_credit_facts.donor_id   AS VARCHAR)),15),16) AS DECIMAL(38,0))) )  AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0) AS "account_credit_balance"
FROM ${users.SQL_TABLE_NAME} AS donor
LEFT JOIN dcdonor  AS donor_ ON donor.personid  = donor_.donorid
LEFT JOIN address  AS donor_address ON donor.addressid = donor_address.addressid
LEFT JOIN hyperlocal_tool.zip_geocode_modified  AS donor_location ON (CASE WHEN (SUBSTRING(donor_address.zip, 1, 5)) ~ '[0-9]{5}' THEN (SUBSTRING(donor_address.zip, 1, 5))::int ELSE NULL END) = donor_location.zip
LEFT JOIN dcteacher  AS teacher_donor ON donor.personid  = teacher_donor.teacherid
LEFT JOIN dbt_target.fct_donor_labels  AS donor_labels ON donor.personid  = donor_labels.donor_id
LEFT JOIN dbt_target.fct_donor_credits  AS donor_credit_facts ON donor.personid  = donor_credit_facts.donor_id
LEFT JOIN dbt_target.fct_payment  AS payment ON payment.donor_id = donor.personid
LEFT JOIN ${cart_window_facts.SQL_TABLE_NAME} AS cart_window_facts ON payment.giving_cart_id = cart_window_facts.cartid
LEFT JOIN dbt_target.fct_check_pledges  AS checks_received ON checks_received.check_pledge_id = payment.check_pledge_id and checks_received.donor_id = payment.donor_id
LEFT JOIN equity.date_spine  AS payment_received_date ON (DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', payment.received_at ))) = (DATE(payment_received_date.the_date ))
LEFT JOIN dcdonation  AS donation ON payment.donation_id  = donation.donationid
LEFT JOIN dbt_target.fct_donations  AS donation_facts ON payment.donation_id  = donation_facts.donation_id
LEFT JOIN matchdonationevent AS dyi_citizen_donation ON dyi_citizen_donation.citizendonationid = donation.donationid
LEFT JOIN matching AS dyi_citizen_match ON dyi_citizen_match.matchingid = dyi_citizen_donation.sponsormatchingid AND dyi_citizen_match.matchingtype = 3
LEFT JOIN dbt_target.fct_payment_referrals  AS referral ON payment.payment_id  = referral.payment_id
LEFT JOIN giftpurchase  AS gift_purchase ON payment.gift_purchase_id  = gift_purchase.id
LEFT JOIN giving_cart_item  AS gift_purchase_cart_item ON gift_purchase.cart_item  = gift_purchase_cart_item.id
LEFT JOIN dbt_target.fct_projects  AS project ON project.project_id = coalesce(payment.project_id, gift_purchase_cart_item.intended_project)
LEFT JOIN ${project_labels.SQL_TABLE_NAME} AS project_labels ON project_labels.projectid = project.project_id
LEFT JOIN dbt_target.fct_essentials_lists  AS essentials_lists_facts ON essentials_lists_facts.essentials_list_id = payment.intended_essentials_list_id
LEFT JOIN ${users.SQL_TABLE_NAME} AS teacher ON COALESCE(project.teacher_id, essentials_lists_facts.teacher_id, (CASE WHEN gift_purchase.batchname LIKE 'DCGIFTCERTIFICATE_TEACH_%' THEN split_part(gift_purchase.batchname, '_TEACH_', 2)::integer END)) = teacher.personid
LEFT JOIN dbt_target.fct_teachers AS teacher_facts ON teacher.personid = teacher_facts.teacher_id
LEFT JOIN school  AS school ON school.schoolid = project.school_id
LEFT JOIN address  AS school_address ON school.addressid = school_address.addressid
LEFT JOIN ${sf_contact_audit.SQL_TABLE_NAME} AS sf_donor_contact ON sf_donor_contact.donor_id_c = donor.personid
WHERE (NOT (donor_labels.grant_account ) OR (donor_labels.grant_account ) IS NULL) AND (NOT (project.is_one_time_sponsor_donor_a_government_entity ) OR (project.is_one_time_sponsor_donor_a_government_entity ) IS NULL)
  AND (donor.personid IN (SELECT donor_id FROM eligible_donors)) 
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    24,
    25,
    26,
    27,
    28,
    29,
    30,
    31,
    32,
    33,
    34