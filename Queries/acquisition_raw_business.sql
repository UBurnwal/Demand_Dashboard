-- Acquisition
WITH
final_raw AS (
SELECT  
A.customer_id,
geo_region_id,
first_order_date
FROM 
lto_ltv_sources_mv a join (select id as customer_id, frequency from customers) c on a.customer_id = c.customer_id
WHERE first_order_date BETWEEN '{start_date}'
-- make change IN DATE
AND '{end_date}' AND frequency = 4),

orders_raw AS (
SELECT 
cso.customer_id,
lto.geo_region_id, 
date_trunc('month',lto.first_order_date)::DATE AS aquisition_month,
date_trunc('month',order_date)::DATE AS order_month,
count(DISTINCT order_id) AS orders,
CASE WHEN count(DISTINCT order_id) <= 2 THEN 'Customer <=2' ELSE 'Customer >=3' END AS customer_flag
FROM 
completed_spot_orders_fast_mv cso
INNER JOIN final_raw lto 
ON cso.customer_id = lto.customer_id 
WHERE order_date >= '{start_date}'
-- make change IN DATE
AND order_date <= '{end_date}'
AND lto.first_order_date >= '{start_date}'
GROUP BY 1,2,3,4),

acq_1 AS (
SELECT 
aquisition_month AS month,
geo_region_id,
COUNT(DISTINCT customer_id) AS customers,
count(DISTINCT CASE WHEN customer_flag = 'Customer <=2' THEN customer_id ELSE NULL END) AS Customer_less_2,
count(DISTINCT CASE WHEN customer_flag = 'Customer >=3' THEN customer_id ELSE NULL END) AS Customer_greater_3,
SUM(orders) AS orders,
sum(CASE WHEN customer_flag = 'Customer <=2' THEN orders ELSE 0 END) AS orders_less_2,
sum(CASE WHEN customer_flag = 'Customer >=3' THEN orders ELSE 0 END) AS orders_greater_3
FROM 
orders_raw
WHERE aquisition_month = order_month
GROUP BY 1,2),

acq_2 AS (
SELECT 
order_month AS month,
geo_region_id,
COUNT(DISTINCT customer_id) AS customers_M_1,
count(DISTINCT CASE WHEN customer_flag = 'Customer <=2' THEN customer_id ELSE NULL END) AS Customer_less_2_M_1,
count(DISTINCT CASE WHEN customer_flag = 'Customer >=3' THEN customer_id ELSE NULL END) AS Customer_greater_3_M_1,
SUM(orders) AS orders_M_1,
sum(CASE WHEN customer_flag = 'Customer <=2' THEN orders ELSE 0 END) AS orders_less_2_M_1,
sum(CASE WHEN customer_flag = 'Customer >=3' THEN orders ELSE 0 END) AS orders_greater_3_M_1
FROM 
orders_raw
WHERE aquisition_month + INTERVAL '1 month' = order_month
GROUP BY 1,2),

acq_3 AS (
SELECT 
order_month AS month,
geo_region_id,
COUNT(DISTINCT ord.customer_id) AS customers_M_2,
count(DISTINCT CASE WHEN customer_flag = 'Customer <=2' THEN ord.customer_id ELSE NULL END) AS Customer_less_2_M_2,
count(DISTINCT CASE WHEN customer_flag = 'Customer >=3' THEN ord.customer_id ELSE NULL END) AS Customer_greater_3_M_2,
SUM(orders) AS orders_M_2,
sum(CASE WHEN customer_flag = 'Customer <=2' THEN ord.orders ELSE 0 END) AS orders_less_2_M_2,
sum(CASE WHEN customer_flag = 'Customer >=3' THEN ord.orders ELSE 0 END) AS orders_greater_3_M_2
FROM 
orders_raw ord
INNER JOIN 
(SELECT DISTINCT customer_id FROM orders_raw WHERE aquisition_month + INTERVAL '1 month' = order_month) a
ON a.customer_id = ord.customer_id 
WHERE aquisition_month + INTERVAL '2 month' = order_month
GROUP BY 1,2)


SELECT * FROM 
	(

SELECT 
acq_1.month, 
acq_1.geo_region_id, 
customers,
Customer_less_2,
Customer_greater_3,
orders,
orders_less_2,
orders_greater_3,
customers_M_1,
Customer_less_2_M_1,
Customer_greater_3_M_1,
orders_M_1,
orders_less_2_M_1,
orders_greater_3_M_1,
customers_M_2
FROM 
acq_1 
LEFT JOIN acq_2
ON acq_1.month = acq_2.month 
AND acq_1.geo_region_id = acq_2.geo_region_id
LEFT JOIN acq_3
ON acq_1.month = acq_3.month 
AND acq_1.geo_region_id = acq_3.geo_region_id

	)  AS A 

WHERE geo_region_id <> 8
OR (geo_region_id = 8 and month >= '2020-09-01')
;