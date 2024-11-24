WITH 
    --how much each user spent per week in the needed period?
     purchases_week AS (SELECT
                          user_pseudo_id,
                          DATE_TRUNC(PARSE_DATE('%Y%m%d',event_date),WEEK) AS purchase_week,
                          SUM(purchase_revenue_in_usd) AS weekly_revenue_in_usd
                        FROM tc-da-1.turing_data_analytics.raw_events
                        WHERE user_pseudo_id IS NOT NULL
                              AND event_name = 'purchase'
                              AND purchase_revenue_in_usd > 0
                              AND (event_date BETWEEN '20201101' AND '20210130')
                        GROUP BY 1, 2),
     --in which week each user registered? (did not use 'first_visit' event only, because part of users do not have such event)
     registrations_week AS (SELECT
                              user_pseudo_id,
                              DATE_TRUNC(PARSE_DATE('%Y%m%d',MIN(event_date)),WEEK) AS registration_week
                            FROM tc-da-1.turing_data_analytics.raw_events
                            WHERE user_pseudo_id IS NOT NULL
                                  AND (event_date BETWEEN '20201101' AND '20210130')
                            GROUP BY 1),
     --how much each user spent per week and which week did they register in?
     purchases_with_reg_date AS (SELECT
                                   pw.user_pseudo_id,
                                   pw.purchase_week,
                                   pw.weekly_revenue_in_usd,
                                   rw.registration_week
                                 FROM purchases_week pw
                                   JOIN registrations_week rw
                                     ON pw.user_pseudo_id = rw.user_pseudo_id),
     --how many users registered each week?
     weekly_registrations AS (SELECT
                                registration_week,
                                COUNT(*) AS weekly_registrations
                              FROM registrations_week
                              GROUP BY registration_week
                              ORDER BY registration_week),
     --what is today's date as given in the task?
     today AS (SELECT
                 DATE('2021-01-24') AS today_date)

--cumulative revenue per customer in 12 weeks after their registration week
--change '<=' to '<' in WHEN conditions for non cumulative revenue
SELECT
  pwrd.registration_week,
  wr.weekly_registrations,
  SUM(pwrd.weekly_revenue_in_usd)/wr.weekly_registrations AS total_revenue,
  SUM(CASE WHEN (pwrd.purchase_week = pwrd.registration_week AND pwrd.registration_week <= today_date) 
           THEN pwrd.weekly_revenue_in_usd 
           ELSE NULL END)/wr.weekly_registrations AS week_0_revenue,
  SUM(CASE WHEN (pwrd.purchase_week <= DATE_ADD(pwrd.registration_week, INTERVAL 1 WEEK) AND DATE_ADD(pwrd.registration_week, INTERVAL 1 WEEK) <= today_date) 
           THEN pwrd.weekly_revenue_in_usd 
           ELSE NULL END)/wr.weekly_registrations AS week_1_revenue,
  SUM(CASE WHEN (pwrd.purchase_week <= DATE_ADD(pwrd.registration_week, INTERVAL 2 WEEK) AND DATE_ADD(pwrd.registration_week, INTERVAL 2 WEEK) <= today_date) 
           THEN pwrd.weekly_revenue_in_usd 
           ELSE NULL END)/wr.weekly_registrations AS week_2_revenue,
  SUM(CASE WHEN (pwrd.purchase_week <= DATE_ADD(pwrd.registration_week, INTERVAL 3 WEEK) AND DATE_ADD(pwrd.registration_week, INTERVAL 3 WEEK) <= today_date) 
           THEN pwrd.weekly_revenue_in_usd 
           ELSE NULL END)/wr.weekly_registrations AS week_3_revenue,
  SUM(CASE WHEN (pwrd.purchase_week <= DATE_ADD(pwrd.registration_week, INTERVAL 4 WEEK) AND DATE_ADD(pwrd.registration_week, INTERVAL 4 WEEK) <= today_date) 
           THEN pwrd.weekly_revenue_in_usd 
           ELSE NULL END)/wr.weekly_registrations AS week_4_revenue,
  SUM(CASE WHEN (pwrd.purchase_week <= DATE_ADD(pwrd.registration_week, INTERVAL 5 WEEK) AND DATE_ADD(pwrd.registration_week, INTERVAL 5 WEEK) <= today_date) 
           THEN pwrd.weekly_revenue_in_usd 
           ELSE NULL END)/wr.weekly_registrations AS week_5_revenue,
  SUM(CASE WHEN (pwrd.purchase_week <= DATE_ADD(pwrd.registration_week, INTERVAL 6 WEEK) AND DATE_ADD(pwrd.registration_week, INTERVAL 6 WEEK) <= today_date) 
           THEN pwrd.weekly_revenue_in_usd 
           ELSE NULL END)/wr.weekly_registrations AS week_6_revenue,
  SUM(CASE WHEN (pwrd.purchase_week <= DATE_ADD(pwrd.registration_week, INTERVAL 7 WEEK) AND DATE_ADD(pwrd.registration_week, INTERVAL 7 WEEK) <= today_date) 
           THEN pwrd.weekly_revenue_in_usd 
           ELSE NULL END)/wr.weekly_registrations AS week_7_revenue,
  SUM(CASE WHEN (pwrd.purchase_week <= DATE_ADD(pwrd.registration_week, INTERVAL 8 WEEK) AND DATE_ADD(pwrd.registration_week, INTERVAL 8 WEEK) <= today_date) 
           THEN pwrd.weekly_revenue_in_usd 
           ELSE NULL END)/wr.weekly_registrations AS week_8_revenue,
  SUM(CASE WHEN (pwrd.purchase_week <= DATE_ADD(pwrd.registration_week, INTERVAL 9 WEEK) AND DATE_ADD(pwrd.registration_week, INTERVAL 9 WEEK) <= today_date) 
           THEN pwrd.weekly_revenue_in_usd 
           ELSE NULL END)/wr.weekly_registrations AS week_9_revenue,
  SUM(CASE WHEN (pwrd.purchase_week <= DATE_ADD(pwrd.registration_week, INTERVAL 10 WEEK) AND DATE_ADD(pwrd.registration_week, INTERVAL 10 WEEK) <= today_date) 
           THEN pwrd.weekly_revenue_in_usd 
           ELSE NULL END)/wr.weekly_registrations AS week_10_revenue,
  SUM(CASE WHEN (pwrd.purchase_week <= DATE_ADD(pwrd.registration_week, INTERVAL 11 WEEK) AND DATE_ADD(pwrd.registration_week, INTERVAL 11 WEEK) <= today_date) 
           THEN pwrd.weekly_revenue_in_usd 
           ELSE NULL END)/wr.weekly_registrations AS week_11_revenue,
  SUM(CASE WHEN (pwrd.purchase_week <= DATE_ADD(pwrd.registration_week, INTERVAL 12 WEEK)  AND DATE_ADD(pwrd.registration_week, INTERVAL 12 WEEK) <= today_date) 
           THEN pwrd.weekly_revenue_in_usd 
           ELSE NULL END)/wr.weekly_registrations AS week_12_revenue
FROM purchases_with_reg_date pwrd, today
  JOIN weekly_registrations wr
    ON pwrd.registration_week = wr.registration_week
GROUP BY pwrd.registration_week, wr.weekly_registrations
ORDER BY pwrd.registration_week;