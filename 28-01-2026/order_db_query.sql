CREATE DATABASE order_db;
GO

USE order_db;
GO

EXEC sp_help 'dbo.orders_data';
GO

SELECT * FROM orders_data;
GO

/*
Task 1: High-Value Order Identification
Finance wants to review unusually high-value orders.
Write a query to fetch all orders where sales are greater than the overall average sales.
*/
SELECT order_id, AVG(sales) as avg_sales
FROM orders_data
GROUP BY order_id
HAVING AVG(sales) > ( SELECT AVG(sales) FROM orders_data );
GO

/*
Task 2: City-Level Revenue Concentration
Management believes that revenue is concentrated in a small number of cities.
Write a query to retrieve the top 5 cities by total sales, ordered from highest to lowest.
*/
SELECT TOP(5) city, SUM(sales) AS total_sales
FROM orders_data
GROUP BY city
ORDER BY SUM(sales) DESC;
GO

/*
Task 3: Customer Purchase Behavior
Marketing wants to identify repeat customers.
Write a query to find customers who have placed more than 5 orders, along with their total sales
*/
SELECT customer_id, SUM(sales) AS total_sales, COUNT(*) AS order_count
FROM orders_data
GROUP BY customer_id
HAVING COUNT(*) > 5;
GO

/*
Task 4: Segment Performance Analysis
Leadership wants to compare customer segments.
Write a query to calculate total sales and total number of orders for each segment, sorted by total sales
*/
SELECT segment, COUNT(*) AS total_orders, SUM(sales) AS total_sales
FROM orders_data
GROUP BY segment
ORDER BY SUM(sales) DESC;
GO

/*
Task 5: Shipping Delay Detection
Operations wants to detect shipment delays.
Write a query to identify orders where the shipping duration exceeds 4 days
 (Ship_Date minus Order_Date greater than 4).
 */
SELECT order_id, DATEDIFF(DAY, order_date, ship_date) AS shipping_duration
FROM orders_data
WHERE DATEDIFF(DAY, order_date, ship_date) > 4;
GO

/*
Task 6: Ship Mode Utilization
Logistics wants to understand how shipping modes are being used.
Write a query to calculate the percentage contribution of each ship mode based on the total number of orders.
*/
WITH ship_mode_group AS (
	SELECT ship_mode, COUNT(*) AS orders
	FROM orders_data
	GROUP BY ship_mode
),
total_order AS (
	SELECT COUNT(*) AS total_orders
	FROM orders_data
)
SELECT ship_mode, (CAST(orders AS FLOAT) / CAST(total_orders AS FLOAT)) * 100 as percentage
FROM ship_mode_group, total_order;
GO

/*
Task 7: City-Level Sales Ranking
Sales leadership wants comparative insights at the city level.
Write a query to rank cities within each country based on total sales using a window function.
*/
SELECT country, city, SUM(sales) AS total_sales,
		DENSE_RANK() OVER(
			PARTITION BY country
			ORDER BY SUM(sales) DESC )
FROM orders_data
GROUP BY country, city;
GO

/*
Task 8: Monthly Order Trend
Management wants to analyze order volume trends over time.
Write a query to calculate the number of orders per month, grouped by year and month using Order_Date.
*/
SELECT YEAR(order_date) AS order_year, MONTH(order_date) AS order_month, COUNT(*) AS orders_per_month
FROM orders_data
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date);
GO

/*
Task 9: Data Quality Validation
Data engineering suspects data inconsistencies.
Write a query to identify orders where the ship date is earlier than the order date
*/
SELECT order_id
FROM orders_data
WHERE ship_date < order_date;
GO






