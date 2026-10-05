CREATE SCHEMA IF NOT EXISTS dw_techpop;

CREATE TABLE IF NOT EXISTS dw_techpop.dim_date (
    date_key int4 NOT NULL,
    full_date date NOT NULL,
    year int4 NOT NULL,
    quarter int4 NOT NULL,
    month int4 NOT NULL,
    day int4 NOT NULL,
    day_of_week int4 NOT NULL,
    is_weekend bool NOT NULL,
    CONSTRAINT dim_date_pkey PRIMARY KEY (date_key)
);

CREATE TABLE IF NOT EXISTS dw_techpop.dim_customer (
    customer_id int4 NOT NULL,
    customer_name varchar(100),
    email varchar(100),
    state varchar(2),
    region varchar(30),
    CONSTRAINT dim_customer_pkey PRIMARY KEY (customer_id)
);

CREATE TABLE IF NOT EXISTS dw_techpop.dim_seller (
    seller_id int4 NOT NULL,
    seller_name varchar(100),
    email varchar(100),
    state varchar(2),
    region varchar(30),
    tx_commission int4,
    CONSTRAINT dim_seller_pkey PRIMARY KEY (seller_id)
);

CREATE TABLE IF NOT EXISTS dw_techpop.dim_product (
    product_id int4 NOT NULL,
    product_name varchar(100),
    list_price numeric(12,2),
    category_id int4,
    category_name varchar(100),
    supplier_id int4,
    supplier_name varchar(100),
    supplier_state varchar(50),
    supplier_region varchar(30),
    CONSTRAINT dim_product_pkey PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS dw_techpop.fact_sales_items (
    sales_id int4 NOT NULL,
    item_id int4 NOT NULL,
    date_key int4,
    customer_id int4,
    seller_id int4,
    product_id int4,
    quantity int4,
    unit_price numeric(12,2),
    line_total numeric(14,2),
    commission_total numeric(14,2),
    CONSTRAINT fact_sales_items_pkey PRIMARY KEY (sales_id, item_id),
    CONSTRAINT fact_sales_items_date_fkey FOREIGN KEY (date_key) REFERENCES dw_techpop.dim_date(date_key),
    CONSTRAINT fact_sales_items_customer_fkey FOREIGN KEY (customer_id) REFERENCES dw_techpop.dim_customer(customer_id),
    CONSTRAINT fact_sales_items_seller_fkey FOREIGN KEY (seller_id) REFERENCES dw_techpop.dim_seller(seller_id),
    CONSTRAINT fact_sales_items_product_fkey FOREIGN KEY (product_id) REFERENCES dw_techpop.dim_product(product_id)
);
