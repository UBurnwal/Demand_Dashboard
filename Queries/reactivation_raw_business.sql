-- Reactivation
WITH MONTH AS (SELECT generate_series( DATE '{start_date}', DATE '{end_date}', '1 month') AS DATE ),
reference_dt AS (SELECT DATE::DATE, (extract(YEAR FROM date)*100 + EXTRACT(MONTH FROM date))::VARCHAR AS MONTH FROM Month),

max_ord_dt AS (
SELECT 
dt.date AS month,
customer_id,
cust.geo_region_id,
max(order_date) As  max_order_dt

FROM completed_spot_orders_fast_mv cso 
INNER JOIN reference_dt dt
ON order_date < dt.date 
LEFT JOIN customers cust 
ON cust.id = cso.customer_id
where frequency = 4

GROUP BY 1,2,3
 ) ,
-- ORDER BY 2,1,3)

raw_data AS (
SELECT 
a.customer_id, 
month_start_date AS order_mnth,
max_order_dt,
CASE WHEN (EXTRACT(YEAR FROM month_start_date) - extract(YEAR FROM max_order_dt))*12 + 
 (EXTRACT(MONTH FROM month_start_date) - extract(MONTH FROM max_order_dt)) > 1 THEN 1 ELSE 0 
END AS reactivation,
(EXTRACT(YEAR FROM month_start_date) - extract(YEAR FROM max_order_dt))*12 + 
 (EXTRACT(MONTH FROM month_start_date) - extract(MONTH FROM max_order_dt)) AS test,
COUNT(order_id) AS orders
FROM 
completed_spot_orders_fast_mv a 
LEFT JOIN max_ord_dt b 
ON a.customer_id = b.customer_id 
AND date_trunc('month',a.order_date) = b.month
WHERE order_date >= (SELECT min(DATE) FROM month) 
GROUP BY 1,2,3,4),

raw_data_M1 AS (
SELECT 
customer_id, 
order_mnth, 
reactivation,
max_order_dt,
orders,
LEAD(order_mnth) OVER(PARTITION by customer_id ORDER BY order_mnth) AS next_mnth,
lead(order_mnth,2) OVER(PARTITION by customer_id ORDER BY order_mnth) AS next_next_mnth, 
LEAD(orders) OVER(PARTITION by customer_id ORDER BY order_mnth) AS nxt_mnth_orders
FROM 
raw_data),

summary_current AS(
SELECT 
order_mnth AS month,
cust.geo_region_id,
count(DISTINCT customer_id) AS reactivated_customers,
count(DISTINCT CASE WHEN orders <= 2 THEN customer_id ELSE NULL end) AS reactivated_l2,
count(DISTINCT CASE WHEN orders >= 3 THEN customer_id ELSE NULL end) AS reactivated_g3,
count(DISTINCT CASE WHEN (EXTRACT(YEAR FROM order_mnth) - EXTRACT(YEAR FROM date_trunc('month',max_order_dt)))*12 + (extract(MONTH FROM order_mnth) - extract(MONTH FROM date_trunc('month',max_order_dt)))  = 2 
    THEN customer_id ELSE NULL end) AS reactivated_g30_l60,
count(DISTINCT CASE WHEN (EXTRACT(YEAR FROM order_mnth) - EXTRACT(YEAR FROM date_trunc('month',max_order_dt)))*12 + (extract(MONTH FROM order_mnth) - extract(MONTH FROM date_trunc('month',max_order_dt)))  = 3 
    THEN customer_id ELSE NULL end) AS reactivated_g60_l90,
count(DISTINCT CASE WHEN (EXTRACT(YEAR FROM order_mnth) - EXTRACT(YEAR FROM date_trunc('month',max_order_dt)))*12 + (extract(MONTH FROM order_mnth) - extract(MONTH FROM date_trunc('month',max_order_dt)))  = 4
    THEN customer_id ELSE NULL end) AS reactivated_g90_l120,
count(DISTINCT CASE WHEN (EXTRACT(YEAR FROM order_mnth) - EXTRACT(YEAR FROM date_trunc('month',max_order_dt)))*12 + (extract(MONTH FROM order_mnth) - extract(MONTH FROM date_trunc('month',max_order_dt)))  > 4 
    THEN customer_id ELSE NULL end) AS reactivated_g120,
sum(orders) AS orders,
sum(CASE WHEN orders <= 2 THEN orders ELSE 0 END) AS orders_l2,
sum(CASE WHEN orders >= 3 THEN orders ELSE 0 END) AS orders_g3

FROM
raw_data_M1 
LEFT JOIN customers cust 
ON raw_data_M1.customer_id = cust.id
WHERE reactivation = 1
GROUP BY 1,2),


summary_current_m1 AS (
SELECT
next_mnth AS month,
cust.geo_region_id, 
count(DISTINCT customer_id) AS reactivated_customers_m1,
count(DISTINCT CASE WHEN orders <= 2 THEN customer_id ELSE NULL end) AS reactivated_l2_m1,
count(DISTINCT CASE WHEN orders >= 3 THEN customer_id ELSE NULL end) AS reactivated_g3_m1,
count(DISTINCT CASE WHEN (EXTRACT(YEAR FROM order_mnth) - EXTRACT(YEAR FROM date_trunc('month',max_order_dt)))*12 + (extract(MONTH FROM order_mnth) - extract(MONTH FROM date_trunc('month',max_order_dt)))  = 2 
    THEN customer_id ELSE NULL end) AS reactivated_g30_l60_m1,
count(DISTINCT CASE WHEN (EXTRACT(YEAR FROM order_mnth) - EXTRACT(YEAR FROM date_trunc('month',max_order_dt)))*12 + (extract(MONTH FROM order_mnth) - extract(MONTH FROM date_trunc('month',max_order_dt)))  = 3 
    THEN customer_id ELSE NULL end) AS reactivated_g60_l90_m1,
count(DISTINCT CASE WHEN (EXTRACT(YEAR FROM order_mnth) - EXTRACT(YEAR FROM date_trunc('month',max_order_dt)))*12 + (extract(MONTH FROM order_mnth) - extract(MONTH FROM date_trunc('month',max_order_dt)))  = 4
    THEN customer_id ELSE NULL end) AS reactivated_g90_l120_m1,
count(DISTINCT CASE WHEN (EXTRACT(YEAR FROM order_mnth) - EXTRACT(YEAR FROM date_trunc('month',max_order_dt)))*12 + (extract(MONTH FROM order_mnth) - extract(MONTH FROM date_trunc('month',max_order_dt)))  > 4 
    THEN customer_id ELSE NULL end) AS reactivated_g120_m1,

sum(orders) AS orders_m1,
sum(CASE WHEN orders <= 2 THEN orders ELSE 0 END) AS orders_l2_m1,
sum(CASE WHEN orders >= 3 THEN orders ELSE 0 END) AS orders_g3_m1
FROM 
raw_data_M1
LEFT JOIN customers cust 
ON raw_data_M1.customer_id = cust.id
WHERE reactivation = 1 
AND (EXTRACT(YEAR FROM next_mnth) - EXTRACT(YEAR FROM order_mnth))*12 + (extract(MONTH FROM next_mnth) - extract(MONTH FROM order_mnth)) = 1
GROUP BY 1,2), 

summary_current_m2 AS (
SELECT
next_next_mnth AS month,
cust.geo_region_id, 
count(DISTINCT a.customer_id) AS reactivated_customers_m2,
count(DISTINCT CASE WHEN orders <= 2 THEN a.customer_id ELSE NULL end) AS reactivated_l2_m2,
count(DISTINCT CASE WHEN orders >= 3 THEN a.customer_id ELSE NULL end) AS reactivated_g3_m2,
count(DISTINCT CASE WHEN (EXTRACT(YEAR FROM order_mnth) - EXTRACT(YEAR FROM date_trunc('month',max_order_dt)))*12 + (extract(MONTH FROM order_mnth) - extract(MONTH FROM date_trunc('month',max_order_dt)))  = 2 
    THEN a.customer_id ELSE NULL end) AS reactivated_g30_l60_m2,
count(DISTINCT CASE WHEN (EXTRACT(YEAR FROM order_mnth) - EXTRACT(YEAR FROM date_trunc('month',max_order_dt)))*12 + (extract(MONTH FROM order_mnth) - extract(MONTH FROM date_trunc('month',max_order_dt)))  = 3 
    THEN a.customer_id ELSE NULL end) AS reactivated_g60_l90_m2,
count(DISTINCT CASE WHEN (EXTRACT(YEAR FROM order_mnth) - EXTRACT(YEAR FROM date_trunc('month',max_order_dt)))*12 + (extract(MONTH FROM order_mnth) - extract(MONTH FROM date_trunc('month',max_order_dt)))  = 4
    THEN a.customer_id ELSE NULL end) AS reactivated_g90_l120_m2,
count(DISTINCT CASE WHEN (EXTRACT(YEAR FROM order_mnth) - EXTRACT(YEAR FROM date_trunc('month',max_order_dt)))*12 + (extract(MONTH FROM order_mnth) - extract(MONTH FROM date_trunc('month',max_order_dt)))  > 4 
    THEN a.customer_id ELSE NULL end) AS reactivated_g120_m2,
sum(orders) AS orders_m2,
sum(CASE WHEN orders <= 2 THEN a.orders ELSE 0 END) AS orders_l2_m2,
sum(CASE WHEN orders >= 3 THEN a.orders ELSE 0 END) AS orders_g3_m2
FROM 
raw_data_M1 a
LEFT JOIN customers cust 
ON a.customer_id = cust.id
INNER JOIN (
SELECT DISTINCT customer_id FROM raw_data_M1 WHERE (EXTRACT(YEAR FROM next_mnth) - EXTRACT(YEAR FROM order_mnth))*12 + (extract(MONTH FROM next_mnth) - extract(MONTH FROM order_mnth)) = 1) b
ON a.customer_id = b.customer_id 
WHERE reactivation = 1 
AND (EXTRACT(YEAR FROM next_next_mnth) - EXTRACT(YEAR FROM order_mnth))*12 + (extract(MONTH FROM next_next_mnth) - extract(MONTH FROM order_mnth)) = 2
GROUP BY 1,2),

accumilated_base AS (
SELECT 
month,
geo_region_id,
count(DISTINCT customer_id) AS acc_customers,
count(DISTINCT CASE WHEN (EXTRACT(YEAR FROM MONTH) - EXTRACT(YEAR FROM date_trunc('month',max_order_dt)))*12 + (extract(MONTH FROM MONTH) - extract(MONTH FROM date_trunc('month',max_order_dt)))  = 2 
    THEN customer_id ELSE NULL end) AS acc_g30_l60,
count(DISTINCT CASE WHEN (EXTRACT(YEAR FROM MONTH) - EXTRACT(YEAR FROM date_trunc('month',max_order_dt)))*12 + (extract(MONTH FROM MONTH) - extract(MONTH FROM date_trunc('month',max_order_dt)))  = 3 
    THEN customer_id ELSE NULL end) AS acc_g60_l90,
count(DISTINCT CASE WHEN (EXTRACT(YEAR FROM MONTH) - EXTRACT(YEAR FROM date_trunc('month',max_order_dt)))*12 + (extract(MONTH FROM MONTH) - extract(MONTH FROM date_trunc('month',max_order_dt)))  = 4
    THEN customer_id ELSE NULL end) AS acc_g90_l120,
count(DISTINCT CASE WHEN (EXTRACT(YEAR FROM MONTH) - EXTRACT(YEAR FROM date_trunc('month',max_order_dt)))*12 + (extract(MONTH FROM MONTH) - extract(MONTH FROM date_trunc('month',max_order_dt)))  > 4 
    THEN customer_id ELSE NULL end) AS acc_g120

FROM 
max_ord_dt 
WHERE 
(EXTRACT(YEAR FROM MONTH) - EXTRACT(YEAR FROM date_trunc('month',max_order_dt)))*12 + (extract(MONTH FROM MONTH) - extract(MONTH FROM date_trunc('month',max_order_dt))) > 1 
GROUP BY 1,2)

select * from (

SELECT * 
FROM 
accumilated_base acc
LEFT JOIN summary_current s1 
ON s1.month = acc.month 
AND s1.geo_region_id = acc.geo_region_id
LEFT JOIN summary_current_m1 s2 
ON s2.month = acc.month 
AND s2.geo_region_id = acc.geo_region_id
LEFT JOIN summary_current_m2 s3 
ON s3.month = acc.month 
AND s3.geo_region_id = acc.geo_region_id
WHERE acc.geo_region_id IS NOT NULL 
AND s1.geo_region_id IS NOT NULL 
AND ( (acc.geo_region_id <> 8 ) OR (acc.geo_region_id = 8 and acc.month >= '2020-09-01') )


    ) as A

;

