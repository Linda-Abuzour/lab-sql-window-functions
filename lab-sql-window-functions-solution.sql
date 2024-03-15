-- lab-sql-window-functions-solution.sql

USE sakila;

-- 1 Rank films by their length and create an output table that includes only the title, length, and rank columns. Filter out any rows with null or zero values in the length column.

SELECT title, length, RANK() OVER (ORDER BY length) AS ranks
FROM film
WHERE length IS NOT NULL AND length > 0;

-- 2 Rank films by length within the rating category and create an output table that includes only the title, length, rating and rank columns. Filter out any rows with null or zero values in the length column.
SELECT title, length, rating, RANK() OVER (PARTITION BY rating ORDER BY length) AS ranks
FROM film
WHERE length IS NOT NULL AND length > 0;

-- 3 Produce a list that shows, for each film in the Sakila database, the actor or actress who has acted in the most number of films, 
-- as well as the total number of films in which they have acted.


-- Create a temporary table movie_counts that selects all the columns from the film_actor table and counts the number of films in which each actor has acted using the COUNT() window function with the OVER() clause that partitions by actor_id. The result is ordered by film_id.
-- This window function applies the COUNT() aggregate function to the film_id column, but instead of aggregating all the rows in the result set, it partitions the rows into separate groups based on the values in the actor_id column.
-- The PARTITION BY clause specifies the column that will be used to create the groups. In this case, it's actor_id.
-- So for each row in the result set, the COUNT() function will count the number of film_id values in the same partition as the current row's actor_id, and return that count as a new column called film_counts.

DROP TEMPORARY TABLE IF EXISTS movie_counts;
CREATE TEMPORARY TABLE movie_counts
SELECT *, COUNT(film_id) OVER (PARTITION BY actor_id) AS film_counts
FROM film_actor
ORDER BY film_id;

-- Create a CTE cte_film_counts_ranks
-- It defines a common table expression (CTE) named cte_film_counts_ranks that selects all columns from the movie_counts table and assigns a rank to each actor within each film based on their number of films they have acted in. The RANK() window function is used with the OVER() clause that partitions by film_id and orders by film_counts in descending order.

WITH cte_film_counts_ranks AS
(
SELECT *, RANK() OVER (PARTITION BY film_id ORDER BY film_counts DESC) AS ranks_
FROM movie_counts
)

-- Finally, it selects the first_name and last_name columns from the actor table, the title column from the film table, and the film_counts column from the cte_film_counts_ranks CTE. 
-- It joins the cte_film_counts_ranks CTE with the actor and film tables on their respective primary keys (actor_id and film_id). It also filters the result set to only include the rows with a rank of 1, which means only the actors who have acted in the most number of films for a given film will be included. The output of this query will show the actor or actress who has acted in the most number of films for each movie in the Sakila database, as well as the total number of films in which they have acted.
SELECT actor.first_name, actor.last_name, film.title, cte_film_counts_ranks.film_counts
FROM cte_film_counts_ranks
JOIN actor ON cte_film_counts_ranks.actor_id = actor.actor_id
JOIN film ON cte_film_counts_ranks.film_id = film.film_id
WHERE ranks_ = 1;

-- This exercise involves analyzing customer activity and retention in the Sakila database to gain insight into business performance. 
-- By analyzing customer behavior over time, businesses can identify trends and make data-driven decisions to improve customer retention and increase revenue.

-- The goal of this exercise is to perform a comprehensive analysis of customer activity and retention by conducting an analysis on the monthly percentage change
--  in the number of active customers and the number of retained customers.
-- Use the Sakila database and progressively build queries to achieve the desired outcome. 

-- Step 1. Retrieve the number of monthly active customers, i.e., the number of unique customers who rented a movie in each month.
-- Step 2. Retrieve the number of active users in the previous month.
-- Step 3. Calculate the percentage change in the number of active customers between the current and previous month.
-- Step 4. Calculate the number of retained customers every month, i.e., customers who rented movies in the current and previous months.


-- 1 Get number of monthly active customers.
WITH customer_activity AS (
	SELECT customer_id, CONVERT(rental_date, DATE) AS activity_date,
		DATE_FORMAT(CONVERT(rental_date,DATE), '%M') AS activity_month,
		DATE_FORMAT(CONVERT(rental_date,DATE), '%Y') AS activity_year
	FROM rental
)
SELECT COUNT(DISTINCT customer_id) AS active_users, activity_year, activity_month
FROM customer_activity
GROUP BY activity_year, activity_month
ORDER BY activity_year, activity_month;

-- 2 Active users in the previous month.

WITH customer_activity AS (
	SELECT customer_id, CONVERT(rental_date, DATE) AS activity_date,
		DATE_FORMAT(CONVERT(rental_date,DATE), '%M') AS activity_month,
		DATE_FORMAT(CONVERT(rental_date,DATE), '%Y') AS activity_year
	FROM rental
),
monthly_active_users AS (
	SELECT COUNT(DISTINCT customer_id) AS active_users, activity_year, activity_month
	FROM customer_activity
	GROUP BY activity_year, activity_Month
	ORDER BY activity_year, activity_Month
),
cte_activity AS (
SELECT active_users, LAG(active_users,1) OVER (PARTITION BY activity_year) AS last_month, activity_year, activity_month
FROM monthly_active_users
)
SELECT * FROM cte_activity
WHERE last_month IS NOT NULL;

-- 3 Percentage change in the number of active customers.

WITH customer_activity AS (
	SELECT customer_id, CONVERT(rental_date, DATE) AS activity_date,
		DATE_FORMAT(CONVERT(rental_date,DATE), '%M') AS activity_month,
		DATE_FORMAT(CONVERT(rental_date,DATE), '%Y') AS activity_year
	FROM rental
),
monthly_active_users AS (
	SELECT COUNT(DISTINCT customer_id) AS active_users, activity_year, activity_month
	FROM customer_activity
	GROUP BY activity_year, activity_Month
	ORDER BY activity_year, activity_Month
),
cte_activity AS (
SELECT active_users, LAG(active_users,1) OVER (PARTITION BY activity_year) AS last_month, activity_year, activity_month
FROM monthly_active_users
)

select (active_users-last_month)/active_users*100 as percentage_change, activity_year, activity_month
from cte_activity
where last_month is not null;

-- 4 Retained customers every month.
WITH customer_activity AS (
	SELECT customer_id, CONVERT(rental_date, DATE) AS activity_date,
		DATE_FORMAT(CONVERT(rental_date,DATE), '%M') AS activity_Month,
		DATE_FORMAT(CONVERT(rental_date,DATE), '%Y') AS activity_year,
		CONVERT(DATE_FORMAT(CONVERT(rental_date,DATE), '%m'), UNSIGNED) AS month_number
	FROM rental
),
distinct_users AS (
	SELECT DISTINCT customer_id , activity_month, activity_year, month_number
	FROM customer_activity
)

SELECT COUNT(DISTINCT d1.customer_id) AS retained_customers, d1.activity_month, d1.activity_year
FROM distinct_users d1
JOIN distinct_users d2 ON d1.customer_id = d2.customer_id AND d1.month_number = d2.month_number + 1
GROUP BY d1.activity_month, d1.activity_year, d1.month_number
ORDER BY d1.activity_year, d1.month_number;
