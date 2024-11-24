WITH min_event_timestamps AS (SELECT 
                                user_pseudo_id,
                                event_name,
                                MIN(event_timestamp) AS min_timestamp
                              FROM tc-da-1.turing_data_analytics.raw_events
                              GROUP BY user_pseudo_id, event_name),
     top_countries AS (SELECT
                         country,
                         ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS country_rank,
                         COUNT(*) AS total_events
                       FROM tc-da-1.turing_data_analytics.raw_events
                       GROUP BY country
                       ORDER BY total_events DESC
                       LIMIT 3),
     events_by_countries AS (SELECT
                               CASE events.event_name
                                 WHEN 'first_visit' THEN 1
                                 WHEN 'view_item' THEN 2
                                 WHEN 'add_to_cart' THEN 3
                                 WHEN 'begin_checkout' THEN 4
                                 WHEN 'add_payment_info' THEN 5
                                 WHEN 'purchase' THEN 6
                                 ELSE 7
                                 END AS event_order,
                               events.event_name,
                               COUNT(DISTINCT IF(top_countries.country_rank = 1, events.user_pseudo_id, NULL)) AS events_country_1,
                               COUNT(DISTINCT IF(top_countries.country_rank = 2, events.user_pseudo_id, NULL)) AS events_country_2,
                               COUNT(DISTINCT IF(top_countries.country_rank = 3, events.user_pseudo_id, NULL)) AS events_country_3,
                               COUNT(DISTINCT events.user_pseudo_id) AS events_top3_countries
                             FROM tc-da-1.turing_data_analytics.raw_events events
                               JOIN min_event_timestamps min_event
                                 ON events.user_pseudo_id = min_event.user_pseudo_id 
                                   AND events.event_name = min_event.event_name 
                                   AND events.event_timestamp = min_event.min_timestamp
                               JOIN top_countries
                                 ON events.country = top_countries.country
                             WHERE events.event_name IN ('first_visit','view_item','add_to_cart','begin_checkout','add_payment_info','purchase')
                             GROUP BY 1, 2)
     
SELECT
  *,
  ROUND(events_top3_countries/MAX(events_top3_countries) OVER (),4) AS perc_top3_countries,
  ROUND(events_country_1/MAX(events_country_1) OVER (),4) AS perc_country_1,
  ROUND(events_country_2/MAX(events_country_2) OVER (),4) AS perc_country_2,
  ROUND(events_country_3/MAX(events_country_3) OVER (),4) AS perc_country_3
FROM events_by_countries
ORDER BY 1;