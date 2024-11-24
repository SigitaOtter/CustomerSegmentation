WITH one_year_data AS (SELECT
                         InvoiceNo,
                         StockCode,
                         Description,
                         Quantity,
                         InvoiceDate,
                         UnitPrice,
                         CustomerID,
                         Country
                       FROM tc-da-1.turing_data_analytics.rfm
                       WHERE CustomerID IS NOT NULL
                             AND InvoiceDate >= '2010-12-01' AND InvoiceDate < '2011-12-02'
                             AND InvoiceNo NOT LIKE 'C%'
                             AND UnitPrice > 0
                      ),
     today_date AS (SELECT DATE('2011-12-01') AS today_date),
     RFM_interim_1 AS (SELECT
                             CustomerID,
                             DATE(MAX(InvoiceDate)) AS last_purchase_date,
                             COUNT(DISTINCT InvoiceNo) AS frequency,
                             SUM(Quantity*UnitPrice) AS monetary
                           FROM one_year_data
                           GROUP BY CustomerID
                          ),
     RFM_interim_2 AS (SELECT
                   *,
                   DATE_DIFF(today_date.today_date, last_purchase_date,DAY) AS recency
                 FROM RFM_interim_1, today_date
                ),
     quantiles AS (SELECT
                     *,
                     r.percentiles[offset(25)] AS r25,
                     r.percentiles[offset(50)] AS r50,
                     r.percentiles[offset(75)] AS r75,
                     r.percentiles[offset(100)] AS r100,
                     f.percentiles[offset(25)] AS f25,
                     f.percentiles[offset(50)] AS f50,
                     f.percentiles[offset(75)] AS f75,
                     f.percentiles[offset(100)] AS f100,
                     m.percentiles[offset(25)] AS m25,
                     m.percentiles[offset(50)] AS m50,
                     m.percentiles[offset(75)] AS m75,
                     m.percentiles[offset(100)] AS m100
                   FROM RFM_interim_2 rfm,
                        (SELECT APPROX_QUANTILES(recency, 100) percentiles FROM RFM_interim_2) r,
                        (SELECT APPROX_QUANTILES(frequency, 100) percentiles FROM RFM_interim_2) f,
                        (SELECT APPROX_QUANTILES(monetary, 100) percentiles FROM RFM_interim_2) m    
                  ),
     rfm_score AS (SELECT
                     *,
                     CASE WHEN recency <= r25 THEN 4
                          WHEN recency <= r50 AND recency > r25 THEN 3 
                          WHEN recency <= r75 AND recency > r50 THEN 2 
                          WHEN recency <= r100 AND recency > r75 THEN 1 
                          END AS r_score,
                     CASE WHEN frequency <= f25 THEN 1
                          WHEN frequency <= f50 AND frequency > f25 THEN 2 
                          WHEN frequency <= f75 AND frequency > f50 THEN 3 
                          WHEN frequency <= f100 AND frequency > f75 THEN 4 
                          END AS f_score,
                     CASE WHEN monetary <= m25 THEN 1
                          WHEN monetary <= m50 AND monetary > m25 THEN 2 
                          WHEN monetary <= m75 AND monetary > m50 THEN 3 
                          WHEN monetary <= m100 AND monetary > m75 THEN 4 
                          END AS m_score
                   FROM quantiles
                  ),
     rfm_comb_score AS (SELECT
                          CustomerID,
                          recency,
                          frequency, 
                          monetary,
                          r_score,
                          f_score,
                          m_score,
                          CAST(ROUND((f_score + m_score) / 2, 0) AS INT64) AS fm_score
                        FROM rfm_score
                        )

SELECT
  *,
  CASE WHEN r_score = 4 AND fm_score = 4 THEN 'Champions'
       WHEN r_score IN (3,4) AND fm_score IN (3,4) THEN 'Loyal Customers'
       WHEN r_score IN (3,4) AND fm_score = 2 THEN 'Potential Loyalist'
       WHEN r_score = 4 AND fm_score = 1 THEN 'New Customers '
       WHEN r_score = 3 AND fm_score = 1 THEN 'Promising'
       WHEN r_score = 2 AND fm_score IN (2,3) THEN 'Need Attention'
       WHEN r_score = 2 AND fm_score = 1 THEN 'About To Sleep'
       WHEN (r_score = 2 AND fm_score = 4) OR (r_score = 1 AND fm_score = 3) THEN 'At Risk'
       WHEN r_score = 1 AND fm_score = 4 THEN 'Cannot Lose Them'
       WHEN r_score = 1 AND fm_score = 2 THEN 'Hibernating'
       WHEN r_score = 1 AND fm_score = 1 THEN 'Lost'
       ELSE 'Other'
       END AS rfm_segment
FROM rfm_comb_score;