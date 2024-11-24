WITH min_event_timestamps AS (SELECT 
                                user_pseudo_id,
                                event_name,
                                MIN(event_timestamp) AS min_timestamp
                              FROM tc-da-1.turing_data_analytics.raw_events
                              GROUP BY user_pseudo_id, event_name),
     events_by_categories AS (SELECT
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
                               COUNT(DISTINCT IF(events.category = 'mobile', events.user_pseudo_id, NULL)) AS events_mobile,
                               COUNT(DISTINCT IF(events.category = 'tablet', events.user_pseudo_id, NULL)) AS events_tablet,
                               COUNT(DISTINCT IF(events.category = 'desktop', events.user_pseudo_id, NULL)) AS events_desktop,
                               COUNT(DISTINCT events.user_pseudo_id) AS events_all_categories
                             FROM tc-da-1.turing_data_analytics.raw_events events
                               JOIN min_event_timestamps min_event
                                 ON events.user_pseudo_id = min_event.user_pseudo_id 
                                   AND events.event_name = min_event.event_name 
                                   AND events.event_timestamp = min_event.min_timestamp
                             WHERE events.event_name IN ('first_visit','view_item','add_to_cart','begin_checkout','add_payment_info','purchase')
                             GROUP BY 1, 2)
     
SELECT
  *,
  ROUND(events_all_categories/MAX(events_all_categories) OVER (),4) AS perc_all_categories,
  ROUND(events_mobile/MAX(events_mobile) OVER (),4) AS perc_mobile,
  ROUND(events_tablet/MAX(events_tablet) OVER (),4) AS perc_tablet,
  ROUND(events_desktop/MAX(events_desktop) OVER (),4) AS perc_desktop
FROM events_by_categories
ORDER BY 1;