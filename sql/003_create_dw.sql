-- Data warehouse (star schema) no schema "dw", no mesmo banco da origem.
-- Origem: schema "public" (001_create_tables.sql).
-- Granularidade da fato: 1 linha por item de venda (sales_items).
-- Script idempotente: pode ser executado mais de uma vez.

CREATE SCHEMA IF NOT EXISTS dw;


-- dw.dim_date: uma linha por dia
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key     int4 NOT NULL,          -- formato yyyymmdd
    full_date    date NOT NULL,
    year         int4 NOT NULL,
    quarter      int4 NOT NULL,
    month        int4 NOT NULL,
    day          int4 NOT NULL,
    day_of_week  int4 NOT NULL,          -- 1 = domingo ... 7 = sabado
    is_weekend   bool NOT NULL,
    CONSTRAINT dim_date_pkey PRIMARY KEY (date_key)
);


-- dw.dim_customer
CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_id    int4 NOT NULL,        -- mesmo id da origem
    customer_name  varchar(100) NULL,
    email          varchar(100) NULL,
    state          varchar(2) NULL,
    CONSTRAINT dim_customer_pkey PRIMARY KEY (customer_id)
);


-- dw.dim_seller
CREATE TABLE IF NOT EXISTS dw.dim_seller (
    seller_id      int4 NOT NULL,        -- mesmo id da origem
    seller_name    varchar(100) NULL,
    email          varchar(100) NULL,
    state          varchar(2) NULL,
    tx_commission  int4 NULL,            -- percentual de comissao
    CONSTRAINT dim_seller_pkey PRIMARY KEY (seller_id)
);


-- dw.dim_product: produto ja desnormalizado com categoria e fornecedor
CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_id      int4 NOT NULL,       -- mesmo id da origem
    product_name    varchar(100) NULL,
    list_price      numeric(12,2) NULL,  -- products.price (preco de tabela)
    category_id     int4 NULL,
    category_name   varchar(100) NULL,
    supplier_id     int4 NULL,
    supplier_name   varchar(100) NULL,
    supplier_state  varchar(50) NULL,
    CONSTRAINT dim_product_pkey PRIMARY KEY (product_id)
);


-- dw.fact_sales_items: 1 linha por item de venda
-- Valor da linha = quantity * unit_price (preco praticado no item).
-- sales.total_price NAO entra na fato: no nivel de item ele seria somado
-- varias vezes (alem disso, nos dados de exemplo ele nao bate com a soma
-- dos itens).
CREATE TABLE IF NOT EXISTS dw.fact_sales_items (
    sales_id     int4 NOT NULL,
    item_id      int4 NOT NULL,
    date_key     int4 NULL,
    customer_id  int4 NULL,
    seller_id    int4 NULL,
    product_id   int4 NULL,
    quantity     int4 NULL,
    unit_price   numeric(12,2) NULL,
    line_total   numeric(14,2) NULL,
    CONSTRAINT fact_sales_items_pkey PRIMARY KEY (sales_id, item_id),
    CONSTRAINT fact_sales_items_date_fkey     FOREIGN KEY (date_key)    REFERENCES dw.dim_date(date_key),
    CONSTRAINT fact_sales_items_customer_fkey FOREIGN KEY (customer_id) REFERENCES dw.dim_customer(customer_id),
    CONSTRAINT fact_sales_items_seller_fkey   FOREIGN KEY (seller_id)   REFERENCES dw.dim_seller(seller_id),
    CONSTRAINT fact_sales_items_product_fkey  FOREIGN KEY (product_id)  REFERENCES dw.dim_product(product_id)
);