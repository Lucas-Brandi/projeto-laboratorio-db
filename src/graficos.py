import os

import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

load_dotenv()


# ============================================================
# CONEXÃO COM O BANCO
# ============================================================

DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_SSLMODE = os.environ.get("DB_SSLMODE", "require")

db_url = URL.create(
    "postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    query={"sslmode": DB_SSLMODE},
)

engine = create_engine(db_url)


# ============================================================
# GRÁFICO 1 - TOP 10 PRODUTOS MAIS VENDIDOS
# ============================================================

query_produtos = """
SELECT
    p.product_name,
    SUM(f.quantity) AS quantity_sold
FROM dw_techpop.fact_sales_items f
JOIN dw_techpop.dim_product p
    ON p.product_id = f.product_id
GROUP BY p.product_name
ORDER BY quantity_sold DESC
LIMIT 10;
"""

df_produtos = pd.read_sql(query_produtos, engine)

fig_produtos = px.bar(
    df_produtos,
    x="quantity_sold",
    y="product_name",
    orientation="h",
    title="Top 10 Produtos Mais Vendidos",
    labels={
        "quantity_sold": "Quantidade Vendida",
        "product_name": "Produto",
    },
)

fig_produtos.update_layout(
    yaxis={"categoryorder": "total ascending"}
)


# ============================================================
# GRÁFICO 2 - RECEITA POR CATEGORIA
# ============================================================

query_categorias = """
SELECT
    p.category_name,
    SUM(f.line_total) AS revenue
FROM dw_techpop.fact_sales_items f
JOIN dw_techpop.dim_product p
    ON p.product_id = f.product_id
GROUP BY p.category_name
ORDER BY revenue DESC;
"""

df_categorias = pd.read_sql(query_categorias, engine)

fig_categorias = px.bar(
    df_categorias,
    x="category_name",
    y="revenue",
    title="Receita por Categoria",
    labels={
        "category_name": "Categoria",
        "revenue": "Receita",
    },
)


# ============================================================
# GRÁFICO 3 - RECEITA AO LONGO DO TEMPO
# ============================================================

query_tempo = """
SELECT
    d.year,
    d.quarter,
    d.month,
    SUM(f.line_total) AS revenue
FROM dw_techpop.fact_sales_items f
JOIN dw_techpop.dim_date d
    ON d.date_key = f.date_key
GROUP BY d.year, d.quarter, d.month
ORDER BY d.year, d.quarter, d.month;
"""

df_tempo = pd.read_sql(query_tempo, engine)

df_tempo["periodo"] = (
    df_tempo["year"].astype(str)
    + "-"
    + df_tempo["month"].astype(str).str.zfill(2)
)

fig_tempo = px.line(
    df_tempo,
    x="periodo",
    y="revenue",
    markers=True,
    title="Receita ao Longo do Tempo",
    labels={
        "periodo": "Período",
        "revenue": "Receita",
    },
)


# ============================================================
# GRÁFICO 4 - RECEITA POR VENDEDOR
# ============================================================

query_vendedores = """
SELECT
    s.seller_name,
    SUM(f.line_total) AS revenue
FROM dw_techpop.fact_sales_items f
JOIN dw_techpop.dim_seller s
    ON s.seller_id = f.seller_id
GROUP BY s.seller_name
ORDER BY revenue DESC;
"""

df_vendedores = pd.read_sql(query_vendedores, engine)

fig_vendedores = px.bar(
    df_vendedores,
    x="seller_name",
    y="revenue",
    title="Receita por Vendedor",
    labels={
        "seller_name": "Vendedor",
        "revenue": "Receita",
    },
)


# ============================================================
# ORGANIZAÇÃO DOS GRÁFICOS
# ============================================================

fig_produtos.update_layout(
    height=500,
    margin=dict(l=40, r=40, t=70, b=40),
)

fig_categorias.update_layout(
    height=500,
    margin=dict(l=40, r=40, t=70, b=40),
)

fig_tempo.update_layout(
    height=500,
    margin=dict(l=40, r=40, t=70, b=40),
)

fig_vendedores.update_layout(
    height=500,
    margin=dict(l=40, r=40, t=70, b=40),
)


# ============================================================
# DASHBOARD
# ============================================================

dashboard = f"""
<!DOCTYPE html>
<html lang="pt-BR">

<head>
    <meta charset="UTF-8">
    <title>Dashboard de Vendas</title>

    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 30px;
            background-color: #f5f6fa;
        }}

        h1 {{
            text-align: center;
            color: #222;
            margin-bottom: 30px;
        }}

        .grafico {{
            background-color: white;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 30px;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
        }}
    </style>
</head>

<body>

    <h1>Dashboard de Vendas</h1>

    <div class="grafico">
        {fig_produtos.to_html(
            full_html=False,
            include_plotlyjs="cdn"
        )}
    </div>

    <div class="grafico">
        {fig_categorias.to_html(
            full_html=False,
            include_plotlyjs=False
        )}
    </div>

    <div class="grafico">
        {fig_tempo.to_html(
            full_html=False,
            include_plotlyjs=False
        )}
    </div>

    <div class="grafico">
        {fig_vendedores.to_html(
            full_html=False,
            include_plotlyjs=False
        )}
    </div>

</body>

</html>
"""


# ============================================================
# SALVAR DASHBOARD
# ============================================================

with open("dashboard.html", "w", encoding="utf-8") as arquivo:
    arquivo.write(dashboard)

print("Dashboard criado com sucesso!")
print("Arquivo: dashboard.html")