# Projeto Spark + PostgreSQL

Pipeline que lê dados de um banco PostgreSQL (hospedado no Aiven) usando
Spark, com processamento em pandas.

## Estrutura

```
.
├── sql/                  # Scripts SQL de criação/alteração de schema
│   └── 001_create_tables.sql
├── src/                  # Código Python
│   ├── init_db.py            # Cria as tabelas no banco a partir de sql/
│   └── spark_read_example.py # Exemplo de leitura via Spark (JDBC)
├── .env.example          # Modelo das variáveis de ambiente (sem valores reais)
├── .gitignore
├── requirements.txt
└── README.md
```

## Setup (rodar uma vez por máquina)

1. Clone o repositório.
2. Crie e ative o ambiente virtual:

   ```bash
   python -m venv venv
   ```

   Ativar no Windows (PowerShell):

   ```powershell
   venv\Scripts\Activate.ps1
   ```

   Ativar no Linux/Mac:

   ```bash
   source venv/bin/activate
   ```

3. Instale as dependências:

   ```bash
   pip install -r requirements.txt
   ```

4. Copie `.env.example` para `.env` e preencha com as credenciais do banco
   (peça no grupo se ainda não tiver — **nunca comite o `.env`**):

   ```bash
   cp .env.example .env
   ```

## Criar as tabelas no banco

Depois de configurar o `.env`:

```bash
python src/init_db.py
```

## Testar a leitura via Spark

```bash
python src/spark_read_example.py
```

> Nota: para o conector JDBC do Postgres funcionar com Spark, pode ser
> necessário baixar o driver `postgresql-*.jar` e apontar o caminho dele
> em `spark_read_example.py` (veja o comentário `spark.jars` no arquivo).

## Convenções do grupo

- Nunca commitar `.env`, pastas de venv, ou credenciais em qualquer formato.
- Alterações de schema vão como novo arquivo em `sql/` (ex:
  `002_add_coluna_x.sql`), nunca editando os arquivos antigos, para manter
  histórico.
- Dependências novas: adicionar em `requirements.txt` com versão fixada.
