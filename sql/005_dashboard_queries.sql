SELECT p.product_name, SUM(f.quantity) AS quantity_sold
FROM dw_techpop.fact_sales_items f
JOIN dw_techpop.dim_product p ON p.product_id = f.product_id
GROUP BY p.product_name
ORDER BY quantity_sold DESC
LIMIT 10;

SELECT SUM(line_total) AS total_revenue
FROM dw_techpop.fact_sales_items;

SELECT p.category_name, p.product_name, SUM(f.line_total) AS revenue
FROM dw_techpop.fact_sales_items f
JOIN dw_techpop.dim_product p ON p.product_id = f.product_id
GROUP BY p.category_name, p.product_name
ORDER BY p.category_name, revenue DESC;

SELECT s.seller_name, SUM(f.commission_total) AS total_commission
FROM dw_techpop.fact_sales_items f
JOIN dw_techpop.dim_seller s ON s.seller_id = f.seller_id
GROUP BY s.seller_name
ORDER BY total_commission DESC;

SELECT supplier_state AS state, COUNT(DISTINCT supplier_id) AS suppliers
FROM dw_techpop.dim_product
GROUP BY supplier_state
ORDER BY suppliers DESC, state;

SELECT state, COUNT(*) AS customers
FROM dw_techpop.dim_customer
GROUP BY state
ORDER BY customers DESC, state;

SELECT d.year, d.quarter, d.month, SUM(f.line_total) AS revenue
FROM dw_techpop.fact_sales_items f
JOIN dw_techpop.dim_date d ON d.date_key = f.date_key
GROUP BY d.year, d.quarter, d.month
ORDER BY d.year, d.quarter, d.month;

SELECT v.category_name, AVG(v.sale_total) AS average_ticket
FROM (
    SELECT f.sales_id, p.category_name, SUM(f.line_total) AS sale_total
    FROM dw_techpop.fact_sales_items f
    JOIN dw_techpop.dim_product p ON p.product_id = f.product_id
    GROUP BY f.sales_id, p.category_name
) v
GROUP BY v.category_name
ORDER BY average_ticket DESC;

SELECT s.seller_name, COUNT(DISTINCT f.sales_id) AS sales, SUM(f.line_total) AS revenue
FROM dw_techpop.fact_sales_items f
JOIN dw_techpop.dim_seller s ON s.seller_id = f.seller_id
GROUP BY s.seller_name
ORDER BY revenue DESC;
