WITH base AS (
  SELECT
    d.personid,
    DATE_TRUNC('day', TRY_CAST(s.start_time AS timestamp))::date AS activity_date,
    TRY_CAST(u.joined_at AS timestamp) AS joined_at,
    s.session_id::varchar AS session_id,
    TRY_CAST(s.duration_in_minutes AS float) AS duration_min,
    TRY_CAST(s.time_to_checkout_in_minutes AS float) AS ttc_min,
    COALESCE(s.utm_medium, '')  AS utm_medium,
    COALESCE(s.utm_source, '')  AS utm_source,
    COALESCE(s.utm_campaign, '')  AS utm_campaign,
    COALESCE(s.referrer, '')    AS referrer,
    COALESCE(s.landing_page, '') AS landing_page,
    COALESCE(s.device_type, '') AS device_type,
    s.region, s.city,
    ROW_NUMBER() OVER (
      PARTITION BY d.personid, DATE_TRUNC('day', TRY_CAST(s.start_time AS timestamp))::date
      ORDER BY CASE WHEN s.region IS NULL OR s.region = '' THEN 1 ELSE 0 END, TRY_CAST(s.start_time AS timestamp) DESC
    ) AS rn_region,
    ROW_NUMBER() OVER (
      PARTITION BY d.personid, DATE_TRUNC('day', TRY_CAST(s.start_time AS timestamp))::date
      ORDER BY CASE WHEN s.city IS NULL OR s.city = '' THEN 1 ELSE 0 END, TRY_CAST(s.start_time AS timestamp) DESC
    ) AS rn_city
  FROM ${heap_sessions.SQL_TABLE_NAME} s
  INNER JOIN ${heap_user.SQL_TABLE_NAME} u
    ON u.heap_user_id = s.heap_user_id
  INNER JOIN ${users.SQL_TABLE_NAME} d
    ON d.personid = u.person_id
)

SELECT
  b.personid as donor_id,
  b.activity_date,

  -- identity & tenure
  DATEDIFF(day, MIN(b.joined_at), b.activity_date) AS tenure_days,
  MAX(CASE WHEN b.rn_region  = 1 THEN b.region  END) AS region,
  MAX(CASE WHEN b.rn_city    = 1 THEN b.city    END) AS city,

  -- session engagement
  COUNT(DISTINCT b.session_id) AS sessions_day,
  SUM(COALESCE(b.duration_min, 0)) AS duration_sum_min,
  SUM(COALESCE(b.ttc_min, 0))      AS time_to_checkout_sum_min,

  -- channel & campaign mix (session-weighted; derived per-session below)
  COALESCE(cs.share_unattributed, 0)    AS share_unattributed,
  COALESCE(cs.share_sharetools, 0) AS share_sharetools,
  COALESCE(cs.share_email, 0)   AS share_email,
  COALESCE(cs.share_ad, 0)   AS share_ad,
  COALESCE(cs.share_other, 0)   AS share_other,

  -- device mode from per-session device
  CASE
    WHEN cs.mobile_ct  >= cs.desktop_ct AND cs.mobile_ct  >= cs.tablet_ct THEN 'Mobile'
    WHEN cs.desktop_ct >= cs.tablet_ct                                   THEN 'Desktop'
    ELSE 'Tablet'
  END AS device_type,

  -- content & intent (via url patterns)
  SUM(CASE WHEN b.landing_page ILIKE '%/project/%'               THEN 1 ELSE 0 END) AS project_page_visits_day,
  SUM(CASE WHEN b.landing_page ILIKE '%/classroom/%'             THEN 1 ELSE 0 END) AS teacher_page_visits_day,
  SUM(CASE WHEN b.landing_page ILIKE '%/donors/search.html%'     THEN 1 ELSE 0 END) AS search_visits_day,
  SUM(CASE WHEN b.landing_page ILIKE '%/donors/givingCart.html%' THEN 1 ELSE 0 END) AS cart_visits_day,

  -- utm & referrer quality
  MAX(CASE WHEN NULLIF(b.utm_campaign, '') IS NOT NULL THEN 1 ELSE 0 END) AS came_from_campaign

FROM base b
INNER JOIN (
  -- per person-day, classify each SESSION once; then compute shares + device counts
  SELECT
    personid,
    activity_date,
    COALESCE(SUM(CASE WHEN session_channel = 'unattributed'    THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0), 0) AS share_unattributed,
    COALESCE(SUM(CASE WHEN session_channel = 'sharetools' THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0), 0) AS share_sharetools,
    COALESCE(SUM(CASE WHEN session_channel = 'email'   THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0), 0) AS share_email,
    COALESCE(SUM(CASE WHEN session_channel = 'ad'   THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0), 0) AS share_ad,
    COALESCE(SUM(CASE WHEN session_channel = 'other'   THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0), 0) AS share_other,
    SUM(CASE WHEN session_device_type = 'Mobile'  THEN 1 ELSE 0 END) AS mobile_ct,
    SUM(CASE WHEN session_device_type = 'Desktop' THEN 1 ELSE 0 END) AS desktop_ct,
    SUM(CASE WHEN session_device_type = 'Tablet'  THEN 1 ELSE 0 END) AS tablet_ct
  FROM (
    SELECT
      personid,
      activity_date,
      session_id,
    CASE
      WHEN MAX(CASE WHEN nullif(utm_source,'') IS NOT NULL THEN 1 ELSE 0 END) = 0
        THEN 'unattributed'
      WHEN MAX(CASE WHEN lower(utm_source) = 'dc' THEN 1 ELSE 0 END) = 1
        THEN 'sharetools'
      WHEN MAX(CASE WHEN lower(utm_medium) = 'email' THEN 1 ELSE 0 END) = 1
        THEN 'email'
      WHEN MAX(CASE WHEN lower(utm_medium) = 'ad' THEN 1 ELSE 0 END) = 1
        THEN 'ad'
      ELSE 'other'
    END AS session_channel,
      CASE
        WHEN MAX(CASE WHEN device_type = 'Mobile'  THEN 1 ELSE 0 END) = 1 THEN 'Mobile'
        WHEN MAX(CASE WHEN device_type = 'Desktop' THEN 1 ELSE 0 END) = 1 THEN 'Desktop'
        WHEN MAX(CASE WHEN device_type = 'Tablet'  THEN 1 ELSE 0 END) = 1 THEN 'Tablet'
        ELSE NULL
      END AS session_device_type
    FROM base
    GROUP BY personid, activity_date, session_id
  ) s
  GROUP BY personid, activity_date
) cs
  ON cs.personid = b.personid
 AND cs.activity_date = b.activity_date
WHERE cs.activity_date >= '2024-07-01'
GROUP BY
  b.personid,
  b.activity_date,
  cs.share_unattributed, cs.share_sharetools, cs.share_email, cs.share_ad, cs.share_other,
  cs.mobile_ct, cs.desktop_ct, cs.tablet_ct
;