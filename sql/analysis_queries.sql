-- =========================================================
-- Analytical Queries on the Sales Data Warehouse (sales_dw)
-- =========================================================

USE sales_dw;

-- 1. Monthly revenue trend
SELECT
    d.year,
    d.month,
    d.month_name,
    ROUND(SUM(f.price + f.freight_value), 2) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM fact_order_items f
JOIN dim_date d ON f.order_date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- 2. Top 10 product categories by revenue
SELECT
    p.category_name_english,
    ROUND(SUM(f.price), 2) AS total_revenue,
    COUNT(*) AS items_sold
FROM fact_order_items f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.category_name_english
ORDER BY total_revenue DESC
LIMIT 10;


-- 3. Top 10 sellers by revenue
SELECT
    s.seller_id,
    s.seller_state,
    ROUND(SUM(f.price), 2) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS orders_handled
FROM fact_order_items f
JOIN dim_seller s ON f.seller_id = s.seller_id
GROUP BY s.seller_id, s.seller_state
ORDER BY total_revenue DESC
LIMIT 10;


-- 4. Revenue and order count by customer state
SELECT
    c.customer_state,
    ROUND(SUM(f.price + f.freight_value), 2) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM fact_order_items f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY total_revenue DESC;


-- 5. Payment type distribution
SELECT
    payment_type,
    COUNT(*) AS num_payments,
    ROUND(SUM(payment_value), 2) AS total_value,
    ROUND(AVG(payment_installments), 1) AS avg_installments
FROM fact_payments
GROUP BY payment_type
ORDER BY total_value DESC;


-- 6. Average review score by product category
SELECT
    p.category_name_english,
    ROUND(AVG(r.review_score), 2) AS avg_review_score,
    COUNT(*) AS num_reviews
FROM fact_reviews r
JOIN fact_order_items f ON r.order_id = f.order_id
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.category_name_english
HAVING num_reviews >= 50
ORDER BY avg_review_score DESC
LIMIT 10;


-- 7. Order status breakdown
SELECT
    order_status,
    COUNT(DISTINCT order_id) AS num_orders
FROM fact_order_items
GROUP BY order_status
ORDER BY num_orders DESC;


-- 8. Average order value (AOV) by month
SELECT
    d.year,
    d.month,
    ROUND(SUM(f.price + f.freight_value) / COUNT(DISTINCT f.order_id), 2) AS avg_order_value
FROM fact_order_items f
JOIN dim_date d ON f.order_date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;


-- 9. Worst-rated products with significant sales volume
SELECT
    p.product_id,
    p.category_name_english,
    ROUND(AVG(r.review_score), 2) AS avg_review_score,
    COUNT(DISTINCT f.order_id) AS num_orders
FROM fact_order_items f
JOIN fact_reviews r ON f.order_id = r.order_id
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.product_id, p.category_name_english
HAVING num_orders >= 10
ORDER BY avg_review_score ASC
LIMIT 10;


-- 10. Freight cost as a percentage of price, by category
SELECT
    p.category_name_english,
    ROUND(AVG(f.freight_value), 2) AS avg_freight,
    ROUND(AVG(f.price), 2) AS avg_price,
    ROUND(AVG(f.freight_value) / AVG(f.price) * 100, 1) AS freight_pct_of_price
FROM fact_order_items f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.category_name_english
ORDER BY freight_pct_of_price DESC
LIMIT 10;