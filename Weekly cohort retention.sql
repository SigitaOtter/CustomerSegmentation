WITH subscribtion_weeks AS (SELECT
                              user_pseudo_id,
                              DATE_TRUNC(subscription_start,WEEK(MONDAY)) AS week_cohort, 
                              subscription_end,
                            FROM tc-da-1.turing_data_analytics.subscriptions),
     today AS (SELECT
                 DATE('2021-02-07') AS today_date)

SELECT 
  week_cohort,
  COUNT(DISTINCT(user_pseudo_id)) AS week0,
  COUNT(DISTINCT IF((subscription_end >= DATE_ADD(week_cohort, INTERVAL 1 WEEK) OR subscription_end IS NULL) AND DATE_ADD(week_cohort, INTERVAL 1 WEEK) <= today_date, user_pseudo_id, NULL)) AS week1,
  COUNT(DISTINCT IF((subscription_end >= DATE_ADD(week_cohort, INTERVAL 2 WEEK) OR subscription_end IS NULL) AND DATE_ADD(week_cohort, INTERVAL 2 WEEK) <= today_date, user_pseudo_id, NULL)) AS week2,
  COUNT(DISTINCT IF((subscription_end >= DATE_ADD(week_cohort, INTERVAL 3 WEEK) OR subscription_end IS NULL) AND DATE_ADD(week_cohort, INTERVAL 3 WEEK) <= today_date, user_pseudo_id, NULL)) AS week3,
  COUNT(DISTINCT IF((subscription_end >= DATE_ADD(week_cohort, INTERVAL 4 WEEK) OR subscription_end IS NULL) AND DATE_ADD(week_cohort, INTERVAL 4 WEEK) <= today_date, user_pseudo_id, NULL)) AS week4,
  COUNT(DISTINCT IF((subscription_end >= DATE_ADD(week_cohort, INTERVAL 5 WEEK) OR subscription_end IS NULL) AND DATE_ADD(week_cohort, INTERVAL 5 WEEK) <= today_date, user_pseudo_id, NULL)) AS week5,
  COUNT(DISTINCT IF((subscription_end >= DATE_ADD(week_cohort, INTERVAL 6 WEEK) OR subscription_end IS NULL) AND DATE_ADD(week_cohort, INTERVAL 6 WEEK) <= today_date, user_pseudo_id, NULL)) AS week6
FROM subscribtion_weeks, today
GROUP BY week_cohort;