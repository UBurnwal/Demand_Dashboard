-- Retention
WITH
mom_data AS 
(

  SELECT cso.*,
  cust.geo_region_id,
  cust.frequency,
  coalesce(ncso.orders_in_next_month,0) AS orders_in_next_month,
  COALESCE(nncso.orders_in_next_to_next_month, 0) AS orders_in_next_to_next_month,
  COALESCE(nnncso.orders_in_nnm, 0) AS orders_in_nnnm
  FROM

  (
    SELECT 
      customer_id , 
      date_trunc('month', order_date) AS current_month,
      (date_trunc('month', order_date) + INTERVAL '1 month') AS next_month,
      (date_trunc('month', order_date) + INTERVAL '2 months') AS next_to_next_month,
     (date_trunc('month', order_date) + INTERVAL '3 months') AS nnn_month,
      count(*) AS orders_in_current_month
    FROM 
      completed_spot_orders_fast_mv
-- change date
      WHERE order_date BETWEEN '{start_date}'AND '{end_date}'
      GROUP BY 1,2,3
  )  AS cso

  LEFT JOIN 
  (SELECT id, geo_region_id, frequency FROM customers) AS cust
  ON cso.customer_id = cust.id
  
  

  LEFT JOIN 

  (
    SELECT customer_id,
      date_trunc('month', order_date) AS next_month,
      count(*) AS orders_in_next_month
    FROM completed_spot_orders_fast_mv
-- change date
    WHERE order_date BETWEEN'{start_date}' AND '{end_date}'
    GROUP BY 1,2
  )  AS ncso

  ON cso.next_month = ncso.next_month
  AND cso.customer_id = ncso.customeR_id

  LEFT JOIN 

  (
    SELECT customer_id,
      date_trunc('month', order_date) AS next_to_next_month,
      count(*) AS orders_in_next_to_next_month
    FROM completed_spot_orders_fast_mv
-- change date
    WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY 1,2
  )  AS nncso

  ON cso.next_to_next_month = nncso.next_to_next_month
  AND cso.customer_id = nncso.customeR_id

  LEFT JOIN 

  (
    SELECT customer_id,
      date_trunc('month', order_date) AS nnnm,
      count(*) AS orders_in_nnm
    FROM completed_spot_orders_fast_mv
-- change date
    WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY 1,2
  )  AS nnncso

  ON cso.nnn_month = nnncso.nnnm
  AND cso.customer_id = nnncso.customeR_id
where frequency = 4

)
,

raw_data_retention AS
(
  SELECT 
    customer_id,
    next_month,
    orders_in_next_month,
    geo_region_id,
    CASE WHEN orders_in_next_month > 0 THEN 1 ELSE 0 END AS retained,
    CASE WHEN orders_in_next_month > 0 AND orders_in_next_month <= 2 THEN 1 ELSE 0 END AS customer_less_than_2,
    CASE WHEN orders_in_next_month > 2 THEN 1 ELSE 0 END AS customer_greater_than_2
  FROM mom_data
  ORDER BY next_month, customer_id
),

retention_table AS 
(
  SELECT 
    next_month::DATE AS month,
    geo_region_id,
    count(DISTINCT customer_id) AS customers_who_ordered_in_previous_month,
    count(DISTINCT customer_id) FILTER (WHERE orders_in_next_month > 0) AS customers_retained,
    COUNT(*) FILTER (WHERE customer_less_than_2 = 1) AS less_than_two_order_customers,
    COUNT(*) FILTER (WHERE customer_greater_than_2 = 1) AS greater_than_two_order_customers,
    SUM(orders_in_next_month) AS total_orders,
    SUM(orders_in_next_month) FILTER (WHERE customer_less_than_2 = 1) AS orders_less_than_2,
    SUM(orders_in_next_month) FILTER (WHERE customer_greater_than_2 = 1) AS orders_greater_than_2
  FROM raw_data_retention
  GROUP BY 1,2 
),

lag_raw_data AS 
(
  SELECT 
    customer_id,
    next_to_next_month,
    orders_in_next_to_next_month,
    orders_in_next_month,
    geo_region_id,
  CASE WHEN orders_in_next_month > 0 AND orders_in_next_to_next_month > 0 THEN 1 ELSE 0 END AS retained,
  CASE WHEN orders_in_next_month > 0 AND orders_in_next_to_next_month > 0  AND orders_in_next_to_next_month <= 2 THEN 1 ELSE 0 END AS customer_less_than_2,
  CASE WHEN orders_in_next_month > 0 AND orders_in_next_to_next_month > 2 THEN 1 ELSE 0 END AS customer_greater_than_2
  FROM mom_data
  ORDER BY next_to_next_month, customer_id
),

retention_table_lag AS 
(
SELECT 
  next_to_next_month::DATE AS month,
  geo_Region_id,
  count(DISTINCT customer_id) AS m_1_customers_who_ordered_in_last_to_last_month,
  count(DISTINCT customer_id) FILTER (WHERE orders_in_next_to_next_month > 0 AND orders_in_next_month > 0) AS m1_customers_retained,
  COUNT(*) FILTER (WHERE customer_less_than_2 = 1) AS m1_less_than_two_order_customers,
  COUNT(*) FILTER (WHERE customer_greater_than_2 = 1) AS m1_greater_than_two_order_customers,
  SUM(orders_in_next_to_next_month) FILTER (WHERE orders_in_next_month > 0) AS m1_total_orders,
  SUM(orders_in_next_to_next_month) FILTER (WHERE customer_less_than_2 = 1) AS m1_orders_less_than_2,
  SUM(orders_in_next_to_next_month) FILTER (WHERE customer_greater_than_2 = 1) AS m1_orders_greater_than_2
FROM lag_raw_data
GROUP BY 1, 2
),

three_month_retention_data AS 
(
    SELECT nnn_month AS month,
    geo_region_id,
    count(DISTINCT customer_id) FILTER (WHERE orders_in_next_month > 0 AND orders_in_next_to_next_month > 0 AND orders_in_nnnm > 0) AS retained_in_next_3_months
    FROM mom_data
    GROUP BY 1,2
)


SELECT * FROM 
  (

SELECT retention_table.*,
retention_table_lag.m_1_customers_who_ordered_in_last_to_last_month,
m1_customers_retained,
m1_less_than_two_order_customers,
m1_greater_than_two_order_customers,
m1_total_orders,
m1_orders_less_than_2,
m1_orders_greater_than_2,
retained_in_next_3_months AS m1_retention_history
FROM 
retention_table
JOIN 
retention_table_lag
ON retention_table_lag.month = retention_table.month
AND retention_table_lag.geo_region_id = retention_table.geo_region_id
JOIN 
three_month_retention_data
ON retention_table_lag.month = three_month_retention_data.month
AND retention_table_lag.geo_region_id = three_month_retention_data.geo_region_id

  ) AS A 

WHERE geo_region_id <> 8
OR (geo_region_id = 8 and month >= '2020-09-01');