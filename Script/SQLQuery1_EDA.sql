----EDA of GOLD database
USE DataWarehouse;
---explore all object in DB
SELECT*FROM INFORMATION_SCHEMA.TABLES

----EXPLOR ALL COLUMNS I DB
SELECT*FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME='dim_customers'

---dim exploration
SELECT DISTINCT country
FROM gold.dim_customers

SELECT DISTINCT category, subcategory, product_name 
FROM gold.dim_products order by 1,2,3

----DATE explore
---finding of first and last order date
---also total years of sales data available
SELECT min(order_date) first_order_date, 
max(order_date) last_order_date,
DATEDIFF(YEAR, min(order_date), mAX(order_date)) AS order_year_range
FROM gold.fact_sales

---YOUNGEST and oldest customer
SELECT
MIN(birthdate) as oldest_birthdate,
DATEDIFF(year, MIN(birthdate), GETDATE()) AS oldest_age,
MAX(birthdate) as yungest_birthdate,
DATEDIFF(year, Max(birthdate), GETDATE()) AS youngest_age
FROM gold.dim_customers

----measures exploration
---total sales
SELECT SUM(sales_amount) AS total_sales 
FROM gold.fact_sales

---how many items are sold
SELECT SUM(quantity) AS total_quantity 
FROM gold.fact_sales

---avg selling price
SELECT avg(price) AS  avg_price
FROM gold.fact_sales

---total number of orders
SELECT COUNT(order_number) AS total_orders
FROM gold.fact_sales
SELECT COUNT(DISTINCT order_number) AS total_distinct_orders
FROM gold.fact_sales
---total no. of products
SELECT COUNT(product_name) total_product 
FROM gold.dim_products

---total no of customers
SELECT COUNT(customer_key) as total_customer
FROM gold.dim_customers

----totla no of customers that has palced the orders
SELECT COUNT(customer_key) TOTAL_CUSTOMERS_placed_order
FROM GOLD.fact_sales

----combining into one table
SELECT 'Total Sales' as measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL 
SELECT 'Total quantity', SUM(quantity) FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', avg(price) FROM gold.fact_sales
UNION ALL
SELECT 'Total no. of Orders', COUNT(order_number) FROM gold.fact_sales
UNION ALL 
SELECT 'Total no. of Products', COUNT(product_name) FROM gold.dim_products
UNION ALL
SELECT 'Total no. of Customers', COUNT(customer_key) FROM gold.dim_customers

---magnitutde analysis
----total no of cuatomer by countries
SELECT
country,
COUNT(customer_key) Total_customers
FROM gold.dim_customers
GROUP BY country
order by Total_customers DESC

----total no of cuatomer by gender
SELECT
gender,
COUNT(customer_key) Total_customers
FROM gold.dim_customers
GROUP BY gender
order by Total_customers DESC

---total products by category
SELECT
category,
COUNT(product_key) Total_products
FROM gold.dim_products
GROUP BY category
order by Total_products DESC

---what is avg cost in each category
SELECT
category,
AVG(cost) Average_costs
FROM gold.dim_products
GROUP BY category
order by Average_costs DESC

-----total revenue generated for each category
SELECT
p.category,
SUM(f.sales_amount) Total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key=f.product_key
GROUP BY p.category
ORDER BY Total_revenue desc

---total revenue generated by each customers
SELECT
c.customer_key,
c.first_name,
c.last_name,
SUM(f.sales_amount) Total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key=f.customer_key
GROUP BY 
c.customer_key,
c.first_name,
c.last_name
ORDER BY Total_revenue DESC

---The distribution of sold items across countries
SELECT
c.country,
SUM(f.quantity) Total_sold_items
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key=f.customer_key
GROUP BY 
c.country
ORDER BY Total_sold_items DESC

----Top 5 products generate the highest revenue

SELECT TOP 5
p.product_name,
SUM(f.sales_amount) total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key=f.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC

----Bottom 5 products generate the lowest revenue

SELECT TOP 5
p.product_name,
SUM(f.sales_amount) total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key=f.product_key
GROUP BY p.product_name
ORDER BY total_revenue

----using window function
----Top 5 products generate the highest revenue
SELECT* FROM(
SELECT
p.product_name,
SUM(f.sales_amount) total_revenue,
ROW_NUMBER() OVER (ORDER BY SUM(f.sales_amount) DESC) rank_products
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key=f.product_key
GROUP BY p.product_name) T
WHERE rank_products<=5

----Top 10 customers who have generate the highest revenue

SELECT TOP 10
c.customer_key,
c.first_name,
c.last_name,
SUM(f.sales_amount) total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON C.customer_key=F.customer_key
GROUP BY
c.customer_key,
c.first_name,
c.last_name
ORDER BY total_revenue DESC

----Lowest 3 customers with the fewest order placed

SELECT TOP 3
c.customer_key,
c.first_name,
c.last_name,
COUNT(DISTINCT order_number) total_orders
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON C.customer_key=F.customer_key
GROUP BY
c.customer_key,
c.first_name,
c.last_name
ORDER BY total_orders



