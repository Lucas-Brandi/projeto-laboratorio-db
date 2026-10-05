# Projeto Spark + PostgreSQL — TechPop

Pipeline de dados que lê as tabelas de vendas do PostgreSQL, transforma os dados com PySpark e carrega um Data Warehouse (DW) em modelo estrela. Ao final, o projeto gera um dashboard HTML interativo com Plotly.

## Fluxo do projeto

```text
Tabelas de origem (public)
        |
        v
ETL PySpark (src/etl_pyspark.py)
        |
        v
DW (dw_techpop: dimensões + fato)
        |
        v
Dashboard Plotly (dashboard.html)
```

1. As tabelas de origem armazenam categorias, clientes, vendedores, fornecedores, produtos, vendas e itens de venda.
2. O ETL cria e limpa o schema `dw_techpop`, lê a origem via JDBC e gera as dimensões e a tabela fato.
3. O script de gráficos consulta o DW e cria o arquivo `dashboard.html` na raiz do repositório.
4. O validador confere estrutura, integridade, regras de negócio, totais e consultas do dashboard.

## Estrutura

```text
.
├── jars/
│   └── postgresql-42.7.13.jar        # Driver JDBC utilizado pelo Spark
├── sql/
│   ├── 001_create_tables.sql         # Estrutura das tabelas de origem
│   ├── 002_insert_data.sql           # Dados de exemplo da origem
│   ├── 003_create_dw.sql             # DDL histórico do DW (schema legado `dw`)
│   ├── 004_create_dw_entrega.sql     # DDL ativo do DW (`dw_techpop`)
│   └── 005_dashboard_queries.sql     # Consultas analíticas do dashboard
├── src/
│   ├── etl_pyspark.py                # Carga da origem para o DW
│   ├── graficos.py                   # Geração do dashboard Plotly
│   ├── init_db.py                    # Execução de scripts SQL selecionados
│   ├── spark_read_example.py         # Exemplo de leitura JDBC com Spark
│   └── validate_dw.py                # Validação do projeto e do DW
├── .env.example                      # Modelo das credenciais
├── requirements.txt                  # Dependências Python fixadas
└── dashboard.html                    # Dashboard gerado
```

> O ETL usa `004_create_dw_entrega.sql`; o arquivo `003_create_dw.sql` é mantido apenas como histórico e não deve ser usado no fluxo atual.

## Dependências

- Python 3.10 ou superior
- Java compatível com PySpark (JDK 8, 11 ou 17 recomendado)
- PostgreSQL acessível com as credenciais configuradas
- Driver JDBC já incluído em `jars/postgresql-42.7.13.jar`

Bibliotecas Python:

- `pyspark==3.5.3`
- `pandas==2.2.3`
- `plotly==7.1.0`
- `psycopg2-binary==2.9.10`
- `sqlalchemy==2.0.36`
- `python-dotenv==1.0.1`

## Instalação

No diretório raiz do projeto, crie e ative um ambiente virtual.

No Windows (PowerShell):

```powershell
py -m venv venv
.\venv\Scripts\Activate.ps1
```

No Linux/macOS:

```bash
python3 -m venv venv
source venv/bin/activate
```

Instale as dependências:

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Configuração do banco

Copie `.env.example` para `.env` e preencha as credenciais do PostgreSQL:

```powershell
Copy-Item .env.example .env
```

```env
DB_HOST=seu-host
DB_PORT=5432
DB_NAME=seu-banco
DB_USER=seu-usuario
DB_PASSWORD=sua-senha
DB_SSLMODE=require
```

O arquivo `.env` contém dados sensíveis e já está ignorado pelo Git. Não o versione.

## Execução passo a passo

### 1. Criar a origem e carregar os dados de exemplo

Execute somente os dois scripts de origem, nesta ordem:

```bash
python src/init_db.py sql/001_create_tables.sql sql/002_insert_data.sql
```

> `002_insert_data.sql` insere dados de exemplo. Em um banco já populado, não o execute novamente sem antes limpar ou usar uma nova base.

### 2. Executar o ETL e criar o DW

```bash
python src/etl_pyspark.py
```

O processo cria o schema `dw_techpop`, executa o DDL ativo e limpa as tabelas do DW antes da carga. Assim, o ETL pode ser executado novamente sem duplicar registros no DW.

O modelo estrela contém:

- `dim_date`
- `dim_customer`
- `dim_seller`
- `dim_product`
- `fact_sales_items` — granularidade de um item por venda

### 3. Gerar o dashboard

```bash
python src/graficos.py
```

O comando sobrescreve `dashboard.html` na raiz. Abra esse arquivo em um navegador com acesso à internet, pois a biblioteca JavaScript do Plotly é carregada por CDN.

O dashboard apresenta:

- Top 10 produtos por quantidade vendida;
- Receita por categoria;
- Receita mensal ao longo do tempo;
- Receita por vendedor.

### 4. Validar o DW

Validação completa, incluindo conexão com o banco:

```bash
python src/validate_dw.py
```

Validação somente dos arquivos e da configuração do projeto, sem conexão:

```bash
python src/validate_dw.py --offline
```

Para tratar avisos como falha, acrescente `--strict`.

## Comandos úteis

Testar apenas a conexão/leitura JDBC do Spark:

```bash
python src/spark_read_example.py
```

Gerar o relatório do validador também em JSON:

```bash
python src/validate_dw.py --json validacao.json
```

## Observações

- A receita no DW é calculada por `quantity * unit_price`, usando o preço praticado em cada item de venda.
- O campo `sales.total_price` não é usado na fato, pois está no nível da venda e poderia ser somado mais de uma vez ao analisar itens.
- As consultas em `sql/005_dashboard_queries.sql` funcionam como referência analítica e são verificadas pelo validador.
