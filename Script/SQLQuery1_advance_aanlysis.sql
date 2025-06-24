USE DataWarehouse;

---Trends and changes over time
--sales performance over time
SELECT
MONTH(order_date) order_month,
YEAR(order_date) order_year,
SUM(sales_amount) Total_sales,
COUNT(DISTINCT customer_key) total_customers,
SUM(quantity) total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date)

--Alternatively
SELECT
DATETRUNC(MONTH, order_date) order_date,
SUM(sales_amount) Total_sales,
COUNT(DISTINCT customer_key) total_customers,
SUM(quantity) total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH, order_date)
ORDER BY DATETRUNC(MONTH, order_date)

---Cumulative analysis---
---total sales per month and 
----running total of sales over time
SELECT
order_date,
total_sales,
SUM(total_sales) OVER(PARTITION BY order_date ORDER BY order_date) AS running_total_sales
FROM(
SELECT
DATETRUNC(MONTH, order_date) order_date,
SUM(sales_amount) Total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH, order_date)
) T

---moving avg 
SELECT
order_date,
total_sales,
SUM(total_sales) OVER(ORDER BY order_date) AS running_total_sales,
AVG(avg_price) OVER (ORDER BY order_date) AS moving_average
FROM(
SELECT
DATETRUNC(YEAR, order_date) order_date,
SUM(sales_amount) Total_sales,
AVG(price) as avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(YEAR, order_date)
) T

---Performance analysis
/*yearly preformance of products by comparing their sales to both avg sales
--performance of the product and previous years sales*/
---CTE
WITH yearly_product_sales AS(
SELECT
YEAR(f.order_date) order_year,
p.product_name,
SUM(f.sales_amount) current_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key=p.product_key
WHERE order_date IS NOT NULL
GROUP BY 
YEAR(f.order_date), p.product_name)

SELECT
order_year,
product_name,
current_sales,
AVG(current_sales) OVER (PARTITION BY product_name ) avg_sales,
current_sales -AVG(current_sales) OVER (PARTITION BY product_name ) avg_diff,
CASE WHEN current_sales -AVG(current_sales) OVER (PARTITION BY product_name) >0 THEN 'Above Avg'
     WHEN current_sales -AVG(current_sales) OVER (PARTITION BY product_name) <0 THEN 'Below Avg'
	 ELSE 'Avg'
END avg_change,
--y-o-y- change
LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) py_sales,
current_sales-LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) diff_py,
CASE WHEN current_sales -LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) >0 THEN 'Increase'
     WHEN current_sales -LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) <0 THEN 'Decrease'
	 ELSE 'No change'
END py_change
FROM yearly_product_sales
ORDER BY product_name, order_year

---part to whole analysis
--which cat contribute the most overall sales

WITH category_sales AS(
SELECT
category,
SUM(sales_amount) total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key=f.product_key
GROUP BY category)

SELECT category, total_sales,
SUM(total_sales) OVER () overall_sales,
CONCAT(ROUND((CAST(total_sales AS FLOAT) /SUM(total_sales) OVER ())*100, 2),'%') PER_of_total_sales
FROM category_sales
ORDER BY total_sales DESC

---Data segmentations
/*segmentqation into cost 
ranges and counts of products fall into each segmant*/

WITH product_segment AS(
SELECT 
product_key,
product_name,
cost,
CASE WHEN cost < 100 THEN 'Below 100'
     WHEN cost BETWEEN 100 AND 500 THEN '100-500'
	 WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
	 ELSE 'Above 1000'
END cost_range
FROM gold.dim_products)

SELECT
cost_range,
COUNT(product_key) total_products
FROM product_segment
GROUP BY cost_range
order by total_products DESC

/* GROUP customers into segments based on thier spending behaviour
VIP: with atleast 12 months of histrory and spending more than $5000
Regular: with atleast 12 months spending but less than $5k
NEW: customers with life span less than 12 months*/
WITH customer_spending AS(
SELECT
c.customer_key,
SUM(f.sales_amount) Total_spending,
MIN(f.order_date) First_order,
MAX(f.order_date) Last_order,
DATEDIFF(MONTH, MIN(f.order_date), MAX(f.order_date)) lifespan
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key=c.customer_key
group by c.customer_key)

SELECT 
customer_segment,
COUNT(customer_key) Total_customers
FROM
(SELECT
customer_key,
CASE WHEN lifespan >=12 AND total_spending >5000 THEN 'VIP'
     WHEN lifespan >=12 AND total_spending <=5000 THEN 'Regular'
	 ELSE 'New'
END customer_segment
FROM customer_spending) T
GROUP BY customer_segment
ORDER BY Total_customers DESC


--- customers report 
CREATE VIEW gold.report_customrs AS
WITH base_query AS(
SELECT
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
c.customer_key,
c.customer_number,
CONCAT(c.first_name, '', c.last_name) AS customer_name,
DATEDIFF(YEAR, c.birthdate, GETDATE()) Age
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
On f.customer_key=c.customer_key
WHERE order_date IS NOT NULL)
, customer_aggregation AS(
---Aggregrate customer metrix
SELECT
customer_key,
customer_number,
customer_name,
age, 
COUNT(DISTINCT order_number) total_orders,
SUM(sales_amount) total_sales,
COUNT(DISTINCT product_key) total_products,
MAX(order_date) last_order_date,
DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) lifespan
FROM base_query
GROUP BY 
customer_key,
customer_number,
customer_name,
age)

SELECT
customer_key,
customer_number,
customer_name,
age,
CASE WHEN age <20  THEN 'Under 20'
     WHEN age between 20 and 29 THEN '20-29'
	 WHEN age between 30 and 39 THEN '30-39'
	 WHEN age between 40 and 49 THEN '40-49'
	 ELSE '50 and above'
END age_group,
CASE WHEN lifespan >=12 AND total_sales >5000 THEN 'VIP'
     WHEN lifespan >=12 AND total_sales <=5000 THEN 'Regular'
	 ELSE 'New'
END customer_segment,
last_order_date,
DATEDIFF(MONTH, last_order_date, GETDATE()) Recency,
total_orders,
total_sales,
total_products,
lifespan,
---compute avg order value
CASE WHEN total_orders=0 then 0
     ELSE total_sales/ total_orders 
END avg_order_value,
--compute avg monthly spends
CASE WHEN lifespan=0 THEN total_sales
     ELSE total_sales/lifespan
END avg_monthly_spends
FROM customer_aggregation

---see the view
SELECT*FROM gold.report_customrs

SELECT
age_group,
count(customer_number) total_customers,
sum(total_sales) total_sales
FROM gold.report_customrs
group by age_group

SELECT
customer_segment,
count(customer_number) total_customers,
sum(total_sales) total_sales
FROM gold.report_customrs
group by customer_segment

----product report
--do as previously